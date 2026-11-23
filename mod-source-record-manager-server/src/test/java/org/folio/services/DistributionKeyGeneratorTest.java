package org.folio.services;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class DistributionKeyGeneratorTest {

  @Test
  public void shouldCycleKeysWithinConfiguredRange() {
    var generator = new DistributionKeyGenerator(3);

    assertEquals("1", generator.next());
    assertEquals("2", generator.next());
    assertEquals("0", generator.next());
    assertEquals("1", generator.next());
  }
}
