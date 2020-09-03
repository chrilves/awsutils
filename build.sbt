enablePlugins(JavaAppPackaging)

// voir http://www.wartremover.org/
lazy val warts = {
  import Wart._
  List(
    ArrayEquals,
    AsInstanceOf,
    Enumeration,
    Equals,
    ExplicitImplicitTypes,
    FinalCaseClass,
    IsInstanceOf,
    JavaSerializable,
    LeakingSealed,
    Null,
    Option2Iterable,
    OptionPartial,
    Product,
    PublicInference,
    Return,
    Serializable,
    StringPlusAny,
    //TraversableOps,
    TryPartial
  )
}

lazy val catsVersion = "2.1.1"
lazy val enumeratumVersion = "1.6.1"
lazy val circeVersion = "0.12.3"

lazy val globalSettings: Seq[sbt.Def.SettingsDefinition] =
  Seq(
    inThisBuild(
      List(
        organization := "com.example",
        scalaVersion := "2.13.3",
        version := "0.1.0-SNAPSHOT"
      )),
    updateOptions := updateOptions.value.withCachedResolution(true),
    //wartremoverErrors in (Compile, compile) := warts,
    //wartremoverWarnings in (Compile, console) := warts,
    addCompilerPlugin("io.tryp" % "splain" % "0.5.7" cross CrossVersion.patch),
    addCompilerPlugin("org.typelevel" %% "kind-projector" % "0.10.3"),
    scalafmtOnCompile := true,
    libraryDependencies ++= Seq(
      "com.chuusai" %% "shapeless" % "2.3.3",
      "org.typelevel" %% "cats-core"   % catsVersion,
      "org.typelevel" %% "cats-effect" % "2.1.4",
      "co.fs2" %% "fs2-core" % "2.4.0",
      "software.amazon.awssdk" % "s3" % "2.13.67",
      "org.scalatest" %% "scalatest" % "3.2.0" % Test
    )
  )

lazy val root =
  project
    .in(file("."))
    .settings(globalSettings:_*)
    .settings(name := "aws-utils")