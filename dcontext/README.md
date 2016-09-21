# Dcontext

Dcontext is a tool for dynamically reloading modified code,
while maintaining a state (e.g. SparkContext, data) in JVM heap.
The purpose of this solution is accelerate software development
productivity in situations, when reloading / reprocessing the application
state is slow.

# Making local installation

In order to publish dcontext locally, run:

```
sbt publish-local
```

# Running tests

Simple test case can be run by running

```
sbt testsh
```

This launches dcontext console. There is single test case, which modifies and uses dcontext state. This test case can be tested in following way:

```
type -h for instructions:
$ -l
test                           fi.veikkaus.dcontext.ScalaTestTask
$ test
this scala task has been run before 0 times
args are: 
$ test
this scala task has been run before 1 times
args are: 
$ test
this scala task has been run before 2 times
args are: 
```

If you want to test the dynamic code reloading, A) run 'sbt ~compile' in a separate terminal, B) modify the ScalaTestTask.scala sources codes and C) rerun the test case.