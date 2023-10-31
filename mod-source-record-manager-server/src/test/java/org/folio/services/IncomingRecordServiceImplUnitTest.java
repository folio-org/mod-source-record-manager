package org.folio.services;

import org.folio.dao.IncomingRecordDao;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.MockitoJUnitRunner;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class IncomingRecordServiceImplUnitTest {

  @Mock
  private IncomingRecordDao incomingRecordDao;

  @InjectMocks
  private IncomingRecordService incomingRecordService = new IncomingRecordServiceImpl();

  @Before
  public void setUp() {
    MockitoAnnotations.openMocks(this);
  }

  @Test
  public void shouldGetById() {
    incomingRecordService.getById(any(), any());
    verify(incomingRecordDao).getById(any(), any());
  }

  @Test
  public void shouldSaveBatch() {
    incomingRecordService.saveBatch(any(), any());
    verify(incomingRecordDao).saveBatch(any(), any());
  }
}
