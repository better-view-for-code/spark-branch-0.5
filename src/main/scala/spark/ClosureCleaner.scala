package spark

import java.lang.reflect.Field

import scala.collection.mutable.Map
import scala.collection.mutable.Set

import org.objectweb.asm.{ClassReader, MethodVisitor, Type}
import org.objectweb.asm.commons.EmptyVisitor
import org.objectweb.asm.Opcodes._

object ClosureCleaner extends Logging {
  // Get an ASM class reader for a given class from the JAR that loaded it
  private def getClassReader(cls: Class[_]): ClassReader = {
    new ClassReader(cls.getResourceAsStream(
      cls.getName.replaceFirst("^.*\\.", "") + ".class"))
  }

  // Check whether a class represents a Scala closure
  private def isClosure(cls: Class[_]): Boolean = {
    cls.getName.contains("$anonfun$")
  }

  // Get a list of the classes of the outer objects of a given closure object, obj;
  // the outer objects are defined as any closures that obj is nested within, plus
  // possibly the class that the outermost closure is in, if any. We stop searching
  // for outer objects beyond that because cloning the user's object is probably
  // not a good idea (whereas we can clone closure objects just fine since we
  // understand how all their fields are used).
  private def getOuterClasses(obj: AnyRef): List[Class[_]] = {
    for (f <- obj.getClass.getDeclaredFields if f.getName == "$outer") {
      f.setAccessible(true)
      if (isClosure(f.getType)) {
        return f.getType :: getOuterClasses(f.get(obj))
      } else {
        return f.getType :: Nil // Stop at the first $outer that is not a closure
      }
    }
    return Nil
  }

  // Get a list of the outer objects for a given closure object.
  private def getOuterObjects(obj: AnyRef): List[AnyRef] = {
    for (f <- obj.getClass.getDeclaredFields if f.getName == "$outer") {
      f.setAccessible(true)
      if (isClosure(f.getType)) {
        return f.get(obj) :: getOuterObjects(f.get(obj))
      } else {
        return f.get(obj) :: Nil // Stop at the first $outer that is not a closure
      }
    }
    return Nil
  }

  private def getInnerClasses(obj: AnyRef): List[Class[_]] = {
    val seen = Set[Class[_]](obj.getClass)
    var stack = List[Class[_]](obj.getClass)
    while (!stack.isEmpty) {
      val cr = getClassReader(stack.head)
      stack = stack.tail
      val set = Set[Class[_]]()
      cr.accept(new InnerClosureFinder(set), 0)
      for (cls <- set -- seen) {
        seen += cls
        stack = cls :: stack
      }
    }
    return (seen - obj.getClass).toList
  }

  private def createNullValue(cls: Class[_]): AnyRef = {
    if (cls.isPrimitive) {
      new java.lang.Byte(0: Byte) // Should be convertible to any primitive type
    } else {
      null
    }
  }

  def clean(func: AnyRef): Unit = {
    // TODO: cache outerClasses / innerClasses / accessedFields
    val outerClasses = getOuterClasses(func)
    val innerClasses = getInnerClasses(func)
    val outerObjects = getOuterObjects(func)

    val accessedFields = Map[Class[_], Set[String]]()
    for (cls <- outerClasses)
      accessedFields(cls) = Set[String]()
    for (cls <- func.getClass :: innerClasses)
      getClassReader(cls).accept(new FieldAccessFinder(accessedFields), 0)
    //logInfo("accessedFields: " + accessedFields)

    val inInterpreter = {
      try {
        val interpClass = Class.forName("spark.repl.Main")
        interpClass.getMethod("interp").invoke(null) != null
      } catch {
        case _: ClassNotFoundException => true
      }
    }

    var outerPairs: List[(Class[_], AnyRef)] = (outerClasses zip outerObjects).reverse
    var outer: AnyRef = null
    if (outerPairs.size > 0 && !isClosure(outerPairs.head._1)) {
      // The closure is ultimately nested inside a class; keep the object of that
      // class without cloning it since we don't want to clone the user's objects.
      outer = outerPairs.head._2
      outerPairs = outerPairs.tail
    }
    // Clone the closure objects themselves, nulling out any fields that are not
    // used in the closure we're working on or any of its inner closures.
    for ((cls, obj) <- outerPairs) {
      outer = instantiateClass(cls, outer, inInterpreter);
      for (fieldName <- accessedFields(cls)) {
        val field = cls.getDeclaredField(fieldName)
        field.setAccessible(true)
        val value = field.get(obj)
        //logInfo("1: Setting " + fieldName + " on " + cls + " to " + value);
        field.set(outer, value)
      }
    }

    if (outer != null) {
      //logInfo("2: Setting $outer on " + func.getClass + " to " + outer);
      val field = func.getClass.getDeclaredField("$outer")
      field.setAccessible(true)
      field.set(func, outer)
    }
  }

  private def instantiateClass(cls: Class[_], outer: AnyRef, inInterpreter: Boolean): AnyRef = {
    //logInfo("Creating a " + cls + " with outer = " + outer)
    if (!inInterpreter) {
      // This is a bona fide closure class, whose constructor has no effects
      // other than to set its fields, so use its constructor
      val cons = cls.getConstructors()(0)
      val params = cons.getParameterTypes.map(createNullValue).toArray
      if (outer != null)
        params(0) = outer // First param is always outer object
      return cons.newInstance(params: _*).asInstanceOf[AnyRef]
    } else {
      // Use reflection to instantiate object without calling constructor
      val rf = sun.reflect.ReflectionFactory.getReflectionFactory();
      val parentCtor = classOf[java.lang.Object].getDeclaredConstructor();
      val newCtor = rf.newConstructorForSerialization(cls, parentCtor)
      val obj = newCtor.newInstance().asInstanceOf[AnyRef];
      if (outer != null) {
        //logInfo("3: Setting $outer on " + cls + " to " + outer);
        val field = cls.getDeclaredField("$outer")
        field.setAccessible(true)
        field.set(obj, outer)
      }
      return obj
    }
  }
}

class FieldAccessFinder(output: Map[Class[_], Set[String]]) extends EmptyVisitor {
  override def visitMethod(access: Int, name: String, desc: String,
                           sig: String, exceptions: Array[String]): MethodVisitor = {
    return new EmptyVisitor {
      override def visitFieldInsn(op: Int, owner: String, name: String, desc: String) {
        if (op == GETFIELD) {
          for (cl <- output.keys if cl.getName == owner.replace('/', '.')) {
            output(cl) += name
          }
        }
      }

      override def visitMethodInsn(op: Int, owner: String, name: String,
                                   desc: String) {
        // Check for calls a getter method for a variable in an interpreter wrapper object.
        // This means that the corresponding field will be accessed, so we should save it.
        if (op == INVOKEVIRTUAL && owner.endsWith("$iwC") && !name.endsWith("$outer")) {
          for (cl <- output.keys if cl.getName == owner.replace('/', '.')) {
            output(cl) += name
          }
        }
      }
    }
  }
}

class InnerClosureFinder(output: Set[Class[_]]) extends EmptyVisitor {
  var myName: String = null

  override def visit(version: Int, access: Int, name: String, sig: String,
                     superName: String, interfaces: Array[String]) {
    myName = name
  }

  override def visitMethod(access: Int, name: String, desc: String,
                           sig: String, exceptions: Array[String]): MethodVisitor = {
    return new EmptyVisitor {
      override def visitMethodInsn(op: Int, owner: String, name: String,
                                   desc: String) {
        val argTypes = Type.getArgumentTypes(desc)
        if (op == INVOKESPECIAL && name == "<init>" && argTypes.length > 0
          && argTypes(0).toString.startsWith("L") // is it an object?
          && argTypes(0).getInternalName == myName)
          output += Class.forName(
            owner.replace('/', '.'),
            false,
            Thread.currentThread.getContextClassLoader)
      }
    }
  }
}
