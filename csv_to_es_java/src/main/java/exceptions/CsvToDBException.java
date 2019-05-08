package exceptions;

/**
 * Created By: garvit
 * Date: 2/5/19
 * Package: exceptions;
 **/

public class CsvToDBException extends Exception {
    public CsvToDBException() {
    }

    public CsvToDBException(String message) {
        super(message);
    }
}
