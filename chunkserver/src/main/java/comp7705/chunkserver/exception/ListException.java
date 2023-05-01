package comp7705.chunkserver.exception;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Reuze
 * @Date 01/05/2023
 */
public class ListException extends Exception {

    private List<Throwable> causes = new ArrayList<>();

    public ListException(List<? extends Throwable> _causes) {
        causes.addAll(_causes);
    }

    public List<Throwable> getException() {
        return causes;
    }
}
