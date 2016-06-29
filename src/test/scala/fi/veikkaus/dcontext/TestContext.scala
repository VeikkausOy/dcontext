package fi.veikkaus.dcontext

import java.util

/**
  * Created by arau on 24.5.2016.
  */
class TestContext
  extends ImmutableDContext(
    Map("test" -> new ScalaTestTask)) {}
