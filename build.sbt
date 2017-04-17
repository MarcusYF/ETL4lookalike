name := "ETL4Lookalike"

version := "1.0"

scalaVersion in Global := "2.10.4"

resolvers += Resolver.mavenLocal

credentials += Credentials(Path.userHome / ".ivy2" / ".credentials")

resolvers += "Sonatype Nexus Repository Manager" at "https://maven.tenddata.com/nexus/content/groups/public/"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.1" withSources()

libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.6.1" withSources()

libraryDependencies += "org.apache.spark" %% "spark-mllib" % "1.6.1" withSources()

//libraryDependencies += "com.talkingdata.dmp" %% "bitmap-ext" % "1.0.3"

//libraryDependencies += "com.talkingdata.dmp" % "ip-location" % "1.0.5"

//libraryDependencies += "com.talkingdata.dmp" %% "data-api" % "1.0.8"

libraryDependencies += "com.talkingdata.dmp" %% "common-lib" % "1.0.3"