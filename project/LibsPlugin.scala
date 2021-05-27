import sbt._
import sbt.Keys._


object LibsPlugin extends AutoPlugin {
  override def trigger  = noTrigger
  override def requires = plugins.JvmPlugin

  object autoImport {
    val libsArchive = taskKey[Unit]("zip libs")
  }

  import autoImport._

  override lazy val projectSettings: Seq[Setting[_]] = Seq(
    libsArchive := {
      val log = streams.value.log
      log.info("Archiving dependencies jars")
      val jars: Seq[(File, String)] =
        (fullClasspath in Runtime).value.files
          .map(f => (f, f.getName))
          .filter(_._2.endsWith(".jar"))

      IO.zip(
        jars,
        (baseDirectory in Compile).value
          / "libs"
          / "lib.zip"
      )
      log.success("Done zipping dependencies lib")
    }
  )

  lazy val zipCommand = Command.command("zip") {(state: State) =>
    val extracted = Project.extract(state)
    val ref       = extracted.get(thisProjectRef)
    extracted.runAggregated(libsArchive in Global in ref, state)
    state
  }
}
