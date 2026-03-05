package chatflow.utils;

import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.regex.Pattern;

public class MessageValidator {

  private static final Pattern USERNAME_PATTERN = Pattern.compile("^[a-zA-Z0-9]{3,20}$");

  public static String validate(ChatMessage m) {

    try {
      int id = Integer.parseInt(m.userId);
      if (id < 1 || id > 100000) {
        return "Invalid userId";
      }
    } catch (NumberFormatException e) {
      return "userId must be numeric";
    }

    if (!USERNAME_PATTERN.matcher(m.username).matches()) {
      return "Invalid username";
    }

    if (m.message == null || m.message.length() < 1 || m.message.length() > 500) {
      return "Invalid message length";
    }

    try {
      Instant.parse(m.timestamp);
    } catch (DateTimeParseException e) {
      return "Invalid ISO timestamp";
    }

    if (!(m.messageType.equals("TEXT")
        || m.messageType.equals("JOIN")
        || m.messageType.equals("LEAVE"))) {
      return "Invalid messageType";
    }

    return null;
  }
}
