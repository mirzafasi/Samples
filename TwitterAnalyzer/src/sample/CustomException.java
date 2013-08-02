package sample;

/**
 * Custom Exception class created for translating exceptions.
 * @author mirza
 *
 */
public class CustomException extends Exception {

	/**
	 * Default UID
	 */
	private static final long serialVersionUID = 1L;

	public CustomException(String message){
		super(message);
	}
	
	public CustomException(String message, Throwable throwable){
		super(message, throwable);
	}
	
	public CustomException(Throwable throwable){
		super(throwable);
	}
}
