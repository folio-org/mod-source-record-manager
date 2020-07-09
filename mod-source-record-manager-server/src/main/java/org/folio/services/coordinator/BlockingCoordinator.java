package org.folio.services.coordinator;

/**
 * Interface for coordination the execution process using locks and solving producer-consumer problem.
 */
public interface BlockingCoordinator {

  /**
   * Proposes to lock a thread to which BlockingCoordinator's instance belongs
   */
  void acceptLock();

  /**
   * Proposes to unlock a thread to which BlockingCoordinator's instance belongs
   */
  void acceptUnlock();
}
