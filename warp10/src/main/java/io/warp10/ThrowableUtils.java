//
//   Copyright 2020  SenX S.A.S.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
//

package io.warp10;

import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class ThrowableUtils {

  /**
   * Minimum size for a single message to be meaningful.
   */
  public static final int MIN_MESSAGE_SIZE = 32;

  public static String getErrorMessage(Throwable t) {
    return getErrorMessage(t, Integer.MAX_VALUE);
  }

  /**
   * Generates an error message composed of each message from the hierarchy of causes.
   *
   * @param t       The root throwable
   * @param maxSize The maximum size of the generated message. Each message will be truncated in the middle if the generated message is longer than maxSize. Must be more than 3, else an IllegalArgumentException is thrown.
   * @return The error message.
   */
  public static String getErrorMessage(Throwable t, int maxSize) {
    String simpleClassName = t.getClass().getSimpleName();

    // Maintain a list of Throwable causes to avoid the unlikely case of a cycle in causes.
    final ArrayList<Throwable> throwables = new ArrayList<>();

    List<String> messages = new ArrayList<String>();

    while (null != t && !throwables.contains(t)) {
      String message = t.getMessage();

      if (null != message) {
        messages.add(message);
      }

      throwables.add(t);
      t = t.getCause();
    }

    String errorMessage = "";

    if (messages.isEmpty() || maxSize < MIN_MESSAGE_SIZE) {
      // In case no message has been found or maxSize is too short, display the Throwable "user-friendly" classname.
      errorMessage = StringUtils.abbreviate(simpleClassName, maxSize);
    } else {
      // Build the message, without caring for maxSize, it will be checked later.
      StringBuilder errorMessageBuilder = buildErrorMessage(messages);

      // Check maxSize
      if (errorMessageBuilder.length() > maxSize) {
        // Check that all messages can be displayed, if not reduce the size of the list of messages
        int maxMessages = maxSize / MIN_MESSAGE_SIZE;
        if (messages.size() > maxMessages) {
          messages = messages.subList(0, maxMessages - 1);
          messages.add("...");
        }

        // Target size of each message, which is the rounded down mean.
        int target = maxSize / messages.size() - 3; // Remove 3 for " ()"

        // Number of character to remove
        int excess = errorMessageBuilder.length() - maxSize;

        // Truncate messages to target length from deeper to shallower, limiting to `excess` removed characters.
        // This results in errors messages deeper in the Exception hierarchy to be truncated more harshly.
        for (int i = messages.size() - 1; i >= 0 && excess > 0; i--) {
          String originalMessage = messages.get(i);
          // Truncate in the middle because the end of the message can be helpful.
          // Ex: when using UPDATE with a very long string value, the root cause is given at the end of exception message returned by Ingress.
          String newMessage = StringUtils.abbreviateMiddle(originalMessage, "...", Math.max(target, originalMessage.length() - excess));
          messages.set(i, newMessage);
          excess -= originalMessage.length() - newMessage.length();
        }

        // Rebuild the whole error message with truncated messages
        errorMessageBuilder = buildErrorMessage(messages);
      }

      errorMessage = errorMessageBuilder.toString();
    }

    // Abbreviate again in case
    return errorMessage;
  }

  /**
   * Build an error message given a collection of messages using the following pattern: msg0 (msg1 (msg2 (msg3)))
   *
   * @param messages The collection of message to use for the generation
   * @return The error message.
   */
  public static StringBuilder buildErrorMessage(Collection<String> messages) {
    StringBuilder errorMessageBuilder = new StringBuilder();


    boolean nextMessage = false;
    for (String message: messages) {
      if (nextMessage) {
        errorMessageBuilder.append(" (");
      } else {
        nextMessage = true;
      }
      errorMessageBuilder.append(message);
    }
    // Close parentheses
    for (int i = 0; i < messages.size() - 1; i++) {
      errorMessageBuilder.append(")");
    }

    return errorMessageBuilder;
  }
}
