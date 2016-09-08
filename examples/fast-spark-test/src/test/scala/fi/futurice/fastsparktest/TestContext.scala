package fi.futurice.fastsparktest

import fi.veikkaus.dcontext.ImmutableDContext

/**
  * Created by arau on 24.5.2016.
  */
class TestContext
  extends ImmutableDContext (
    Map("test" -> new ExampleTest)) {}


