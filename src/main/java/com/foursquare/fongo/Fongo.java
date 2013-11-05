package com.foursquare.fongo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @deprecated use {@link com.github.fakemongo.Fongo} instead
 */
@Deprecated
public class Fongo extends com.github.fakemongo.Fongo {
  private static final Logger LOG = LoggerFactory.getLogger(Fongo.class);

  /**
   * @param name Used only for a nice toString in case you have multiple instances
   * @deprecated use {@link com.github.fakemongo.Fongo} instead
   */
  @Deprecated
  public Fongo(String name) {
    super(name);
    LOG.error("This class is deprecated, use com.github.fakemongo.Fongo instead.");
  }
}
