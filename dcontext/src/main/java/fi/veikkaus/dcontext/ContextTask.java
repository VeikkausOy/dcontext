package fi.veikkaus.dcontext;

/**
 * Created by arau on 28.6.2016.
 */
public interface ContextTask {
    void run(MutableDContext context, String[] args);
}
