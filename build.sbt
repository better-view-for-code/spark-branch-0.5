import sbt.Keys.commands

val slf4jVersion = "1.6.1"
val HADOOP_VERSION = "0.20.205.0"
val akka_version = "2.6.4"

lazy val root = (project in file("."))
  .enablePlugins(LibsPlugin)
  .settings(
    name := "spark-base",
    version := "0.1",
    scalaVersion := "2.12.8",
    libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-actor" % akka_version,
      "com.typesafe.akka" %% "akka-cluster-sharding" % akka_version,
      "com.typesafe.akka" %% "akka-remote" % akka_version,
      "com.google.guava" % "guava" % "11.0.1",
      "log4j" % "log4j" % "1.2.16",
      "org.slf4j" % "slf4j-api" % slf4jVersion,
      "org.slf4j" % "slf4j-log4j12" % slf4jVersion,
      "com.ning" % "compress-lzf" % "0.8.4",
      "org.apache.hadoop" % "hadoop-core" % HADOOP_VERSION,
      "asm" % "asm-all" % "3.3.1",
      "com.google.protobuf" % "protobuf-java" % "2.4.1",
      "de.javakaffee" % "kryo-serializers" % "0.9",
      "org.jboss.netty" % "netty" % "3.2.6.Final",
      "it.unimi.dsi" % "fastutil" % "6.4.2",
      "colt" % "colt" % "1.2.0",
      "org.apache.mesos" % "mesos" % "0.9.0-incubating",
      "org.eclipse.jetty" % "jetty-server" % "7.5.3.v20111011"
    ),

    commands ++= Seq(LibsPlugin.zipCommand),

    resolvers ++= Seq(
      "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/",
      "JBoss Repository" at "http://repository.jboss.org/nexus/content/repositories/releases/",
      "Cloudera Repository" at "http://repository.cloudera.com/artifactory/cloudera-repos/"
    )
  )
