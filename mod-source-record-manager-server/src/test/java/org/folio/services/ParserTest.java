package org.folio.services;

import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.folio.services.parsers.ParsedResult;
import org.folio.services.parsers.RawRecordParser;
import org.folio.services.parsers.RawRecordParserBuilder;
import org.folio.services.parsers.RecordFormat;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * REST tests for MetadataProvider to manager JobExecution entities
 */
@RunWith(VertxUnitRunner.class)
public class ParserTest {
  private static final RawRecordParser PARSER = RawRecordParserBuilder.buildParser(RecordFormat.MARC);
  private static final String CORRECT_RECORD = "01240cas a2200397   450000100070000000500170000700800410002401000170006" +
    "502200140008203500260009603500220012203500110014403500190015504000440017405000150021808200110023322200420024424500" +
    "4300286260004700329265003800376300001500414310002200429321002500451362002300476570002900499650003300528650004500561" +
    "6550042006067000045006488530018006938630023007119020016007349050021007509480037007719500034008083668322014110622" +
    "1425.0750907c19509999enkqr p       0   a0eng d  a   58020553   a0022-0469  a(CStRLIN)NYCX1604275S  a(NIC)not" +
    "isABP6388  a366832  a(OCoLC)1604275  dCtYdMBTIdCtYdMBTIdNICdCStRLINdNIC0 aBR140b.J6  a270.0504aThe Jou" +
    "rnal of ecclesiastical history04aThe Journal of ecclesiastical history.  aLondon,bCambridge University Press [e" +
    "tc.]  a32 East 57th St., New York, 10022  av.b25 cm.  aQuarterly,b1970-  aSemiannual,b1950-690 av. 1-   Ap" +
    "r. 1950-  aEditor:   C. W. Dugmore. 0aChurch historyxPeriodicals. 7aChurch history2fast0(OCoLC)fst00860740 7" +
    "aPeriodicals2fast0(OCoLC)fst014116411 aDugmore, C. W.q(Clifford William),eed.0381av.i(year)4081a1-49i1950-" +
    "1998  apfndbLintz  a19890510120000.02 a20141106bmdbatcheltsxaddfast  lOLINaBR140b.J86h01/01/01 N01542ccm" +
    " a2200361   ";
  private static final String UNCORRECT_RECORD = "01240cas a2200397   45000010007000000050017000070080";
  private static final String EMPTY_RECORD = "01240cas a2200397   45000010007000000050017000070080";
  private static final String NULL_RECORD = null;

  @Test
  public void parseRecord(TestContext testContext) {
    ParsedResult result = PARSER.parseRecord(CORRECT_RECORD);
    testContext.assertFalse(result.isHasError());
    testContext.assertNotNull(result.getParsedRecord());
    testContext.assertNotEquals(result.getParsedRecord().encode(), "");

  }

  @Test
  public void parseErrorSource(TestContext testContext) {
    ParsedResult result = PARSER.parseRecord(UNCORRECT_RECORD);
    testContext.assertTrue(result.isHasError());
    testContext.assertNotNull(result.getErrors());
    testContext.assertNotEquals(result.getErrors().encode(), "");
  }

  @Test
  public void parsEmptySource(TestContext testContext) {
    ParsedResult result = PARSER.parseRecord(EMPTY_RECORD);
    testContext.assertTrue(result.isHasError());
    testContext.assertNotNull(result.getErrors());
    testContext.assertNotEquals(result.getErrors().encode(), "");
  }

  @Test
  public void parsNullSource(TestContext testContext) {
    ParsedResult result = PARSER.parseRecord(NULL_RECORD);
    testContext.assertTrue(result.isHasError());
    testContext.assertNotNull(result.getErrors());
    testContext.assertNotEquals(result.getErrors().encode(), "");
  }
}
