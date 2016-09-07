package fi.veikkaus.dcontext

class TestContext
  extends ImmutableDContext(
    Map("test" -> new ScalaTestTask)) {}
