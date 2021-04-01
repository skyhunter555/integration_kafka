package ru.syntez.integration.kafka.exceptions;

/**
 * Wrapper over RuntimeException. Includes additional options for formatting message text.
 *
 * @author Skyhunter
 * @date 26.02.2021
 */
public class TestMessageNotFoundException extends RuntimeException {

    public TestMessageNotFoundException(String message) {
        super(message);
    }

    public TestMessageNotFoundException(String messageFormat, Object... messageArgs) {
        super(String.format(messageFormat, messageArgs));
    }

    public TestMessageNotFoundException(Throwable throwable) {
        super(throwable);
    }

}
