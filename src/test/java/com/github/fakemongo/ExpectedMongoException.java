package com.github.fakemongo;

import com.mongodb.CommandFailureException;
import com.mongodb.MongoException;
import com.mongodb.WriteConcernException;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.rules.ExpectedException;

public final class ExpectedMongoException {

  private ExpectedMongoException() {
  }

  public static ExpectedException expectCommandFailure(ExpectedException expectedException, int code) {
    return expectCode(expectedException, code, CommandFailureException.class);
  }

  public static ExpectedException expectWriteConcernException(ExpectedException expectedException, int code) {
    return expectCode(expectedException, code, WriteConcernException.class);
  }

  public static ExpectedException expect(ExpectedException expectedException, Class<? extends MongoException> exception) {
    expectedException.expect(exception);
    return expectedException;
  }

  public static ExpectedException expectCode(ExpectedException expectedException, int code) {
    expectedException.expect(equalCode(code));
    return expectedException;
  }

  public static ExpectedException expectCode(ExpectedException expectedException, int code, Class<? extends MongoException> exception) {
    expectedException.expect(exception);
    expectedException.expect(equalCode(code));
    return expectedException;
  }

  private static Matcher<Throwable> equalCode(final int code) {
    return new TypeSafeMatcher<Throwable>() {
      public void describeTo(Description description) {
        description.appendText("exception must have code " + code);
      }

      @Override
      public boolean matchesSafely(Throwable item) {
        MongoException mongoException = (MongoException) item;
        return mongoException.getCode() == code;
      }
    };
  }

}
