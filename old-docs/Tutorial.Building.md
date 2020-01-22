# Building CM-Well #

----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](Tutorial.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](Tutorial.HandsOnExercisesTOC.md)  

----


This tutorial describes how to build CM-Well from scratch. It assumes you don't have the source, scala or SBT installed but are comfortable with UNIX command line.

>**Note:** All commands/output shown were tested on a Mac running 10.12.5. Your mileage may vary.

## Prerequisites ##

CM-Well requires the following prerequisite software to install.

### Scala ###

Build and run is quite sensitive to the version of Scala. Currently, we build and test using Scala [2.12.4](https://www.scala-lang.org/download/2.12.4.html).

Once you've unpackaged the download, move the folder to a convenient directory, noting that we have to make a slight change to the Scala install so best to use a dedicated copy for CM-Well. In this example, we install Scala to the home directory.

Next, set your SCALA_HOME variable:
```
$ export SCALA_HOME=~/scala-2.12.4
```

Add the Scala binary directory to your path
```
$ PATH=$PATH:$SCALA_HOME/bin;export PATH
```

Verify that scala is available and running:

```
$ scala -version
Scala code runner version 2.12.4 -- Copyright 2002-2017, LAMP/EPFL
```

### Install Scala Build Tool (SBT) ###

Next we need [SBT](http://www.scala-sbt.org). For this tutorial, we downloaded the [binaries](https://github.com/sbt/sbt/releases/download/v1.1.4/sbt-1.1.4.zip) directly and unzipped alongside the Scala directory. This tutorial was tested with version 1.1.4.

Like Scala, add sbt to your path:
```
$ PATH=$PATH:~/sbt/bin;export PATH
```

Confirm SBT runs:
```
$ sbt about
WARN: No sbt.version set in project/build.properties, base directory: /Users/TRnonodename/Code
[warn] Executing in batch mode.
[warn]   For better performance, hit [ENTER] to switch to interactive mode, or
[warn]   consider launching sbt without any commands, or explicitly passing 'shell'
[info] Set current project to code (in build file:/Users/TRnonodename/Code/)
[info] This is sbt 0.13.15
[info] The current project is {file:/Users/TRnonodename/Code/}code 0.1-SNAPSHOT
[info] The current project is built against Scala 2.12.4
[info] Available Plugins: sbt.plugins.IvyPlugin, sbt.plugins.JvmPlugin, sbt.plugins.CorePlugin, sbt.plugins.JUnitXmlReportPlugin, sbt.plugins.Giter8TemplatePlugin
[info] sbt, sbt plugins, and build definitions are using Scala 2.12.4
```
## Installing CM-Well ##

>**Note:** On the machine from which you're installing CM-Well, you'll first need to install [Python 2.7](https://www.python.org/download/releases/2.7).

[Download](https://github.com/CM-Well/CM-Well/archive/master.zip) or clone the source from Github. Once you have the source, navigate into the server directory:
```
$ cd CM-Well/server/
```
Next, we need to give SBT more memory to work with. There's a couple of ways to do this, setting an options variable works well:
```
export SBT_OPTS="-Xmx2G -XX:+UseConcMarkSweepGC -XX:+CMSClassUnloadingEnabled -Xss2M"
```

Or you can configure sbt by issuing a -mem command from the server directory
```
$ sbt -mem 2048
[info] Loading project definition from /Users/TRnonodename/Code/CM-Well/server/project/project
[info] Loading project definition from /Users/TRnonodename/Code/CM-Well/server/project
[info] Resolving key references (19979 settings) ...
[info] Set current project to server (in build file:/Users/TRnonodename/Code/CM-Well/server/)
server> exit
```
Now kick off a build by calling sbt with the packageCmwell command
```
$ sbt packageCmwell
Getting org.scala-sbt sbt 0.13.15  (this may take some time)...
downloading file:////Users/TRNonodename/.sbt/preloaded/org.scala-sbt/sbt/0.13.15/jars/sbt.jar ...
	[SUCCESSFUL ] org.scala-sbt#sbt;0.13.15!sbt.jar (8ms)
...
[info] 	/Users/TRNonodename/Code/CM-Well/server/cmwell-cons/app/components/zookeeper-3.4.6.tar.gz
[success] Total time: 247 s, completed Jul 21, 2017 11:09:07 AM
```

>**Note:** The first build will take some time while SBT downloads all dependencies.

## Running CM-Well ##

Once you have the server compiled, navigate to the cmwell-cons/app directory and run cmwell.sh

```
$ cd cmwell-cons/app/
$ ./cmwell.sh
-e
                         Welcome to CM-Well Console


                             .-::/++ooooo++//:-.`                               
                         .:/oooooo+oo+++o++oooooo+/:.`                          
                      .:+oo+++++o+ooooooo++++++++++ooo/-`                       
                    ./oo++++oooooooooooo+++++//////:/++oo/-`                    
                  `/o++++oooooooooooo+++++++////::::::-:/+o+:`                  
                 :oo+++/:-----::/+o++++++//////::::-----.-:+oo/`                
               .+ooo+-.....---::-.--:///////:::::----.......-+o+:               
             `./o+:.````..----://:-   ``.-::::-----......`````:+o+.             
           `---o+`  . `...----:/::-.       `..---.....```````` ./oo-            
          ..-`+o-  ````..----://:++/:-.``     ``....``````..``` `:oo:           
        `:`.`-o+   `.`..--::///::--:://///:-.``  ``` `...----:/:. -oo:          
       `:. -`/o:    .--:://////:--.`   `.-://+/:-..`.``..----://:-`-oo-         
      `.- `-`+o-     .:///////::-.`         ``.-://+/:-..----:/::-- :oo.        
      -.` .  /o.       `-:://:-.`                 ..-/:.----://:---  +o/        
     ..-  .  /o-          `:+/                    `.`.---:////::-.-  .oo.       
     :`-  .  -o:           `/o.                    .:////////::---`   +o:       
    `` .  .  `o+`           -++                   ./o+://////::-.`    /o+       
    .  .  ..  /o:           `:o:                ./o+-``.--:--.``      -oo       
    .` -`.`-  `+o`           ./o`             ./o+-`                  -o+       
     . ..  .`  -o+`           :+/           ./o/-`                    :o/       
     -  .   .   :o/`          `/o-````    ./o/-`                      +o-       
     .` .`   .`.`:o/`         `-+o----:-./o/-`                       -o+`       
      -  .   `-   -++.      ```.:+/---//:/-`                        `+o-        
      `. `-   `.`  ./o:`   .```.-:---://:--                      ```/o/         
       `-.`.`  `.` ``:++-` . `...---://::--.                  ````./o/          
        `.  .`  `..```./o+--`..---:///::-..`               `````.-+o:           
         `.` ..```.:.```-/+o/::://////::-..               ```..-/o/.            
           ..``---.`.--...-:+o++/////::--.              ``..-:+o+-              
            `..`.-.....-:----:/+oo++/:-.               ..-/+o+/.                
              `-...---.--:::::////++oo+/:-..```````.--/+oo+/-`                  
                `--.-:::---::////////++++ooooooooooo+o+/:.                      
                  `.-:-:////:////++++++++++o++ooo+++/-`                         
                     `.::////+/+++++++ooooooooo++:.                             
                         `.--:/+++++ooooo++/:-.`                                
                                 ````````                                       

Welcome to Scala 2.12.4 (Java HotSpot(TM) 64-Bit Server VM, Java 1.8.0_45).
Type in expressions for evaluation. Or try :help.
```

Once the console has loaded, start CM-Well running:
```
scala> :load pe
Loading pe...
import scala.language.postfixOps
locParam: String = null
location: String = /Users/TRnonodename/app
useAuthorization: Boolean = false
dataDirs: DataDirs = DataDirs(List(/Users/TRnonodename/app/cm-well-data/cas),List(/Users/TRnonodename/app/cm-well-data/ccl),List(/Users/TRnonodename/app/cm-well-data/es),List(/Users/TRnonodename/app/cm-well-data/tlog),/Users/TRnonodename/app/cm-well-data/kafka,/Users/TRnonodename/app/cm-well-data/zookeeper,/Users/TRnonodename/app/cm-well-data/log)
pe: LocalHost = LocalHost(lh,DataDirs(List(/Users/TRnonodename/app/cm-well-data/cas),List(/Users/TRnonodename/app/cm-well-data/ccl),List(/Users/TRnonodename/app/cm-well-data/es),List(/Users/TRnonodename/app/cm-well-data/tlog),/Users/TRnonodename/app/cm-well-data/kafka,/Users/TRnonodename/app/cm-well-data/zookeeper,/Users/TRnonodename/app/cm-well-data/log),InstDirs(/Users/TRnonodename/app/cm-well,/Users/TRnonodename/app),false,false,0,DevAllocations(),false,true,true,false,true,false)
pe.mappingFile: String = mapping-pe.json

scala> pe.install
Info: purging cm-well
Info:   stopping processes
Info:   clearing application data
...
Info: finished initializing cm-well
scala>
```
Once the scala prompt returns, CM-Well is running, listening to port 9000. Navigate your browser to [http://localhost:9000](http://localhost:9000) and you should see the root UI.

From here on in, you can use curl to load data per the other tutorials. Note that no data is persisted to this copy beyond a restart of CM-Well.

### Stopping CM-Well ###

To shut CM-Well down, first shutdown CM-Well with pe.stop

```
scala> pe.stop
```

Then issue a :quit command to exit the console:

```
scala> :quit
```

----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](Tutorial.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](Tutorial.HandsOnExercisesTOC.md)  

----
