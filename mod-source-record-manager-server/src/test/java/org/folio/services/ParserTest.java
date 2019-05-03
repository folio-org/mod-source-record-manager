package org.folio.services;

import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.folio.services.parsers.ParsedResult;
import org.folio.services.parsers.RecordParser;
import org.folio.services.parsers.RecordParserBuilder;
import org.folio.services.parsers.RecordFormat;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(VertxUnitRunner.class)
public class ParserTest {

  private static final String RAW_MARC_RECORD = "01240cas a2200397   450000100070000000500170000700800410002401000170006" +
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
  private static final String JSON_MARC_RECORD = "{\"leader\":\"01542ccm a2200361   4500\",\"fields\":[{\"001\":\"393893\"},{\"005\":\"20141107001016.0\"}," +
    "{\"008\":\"830419m19559999gw mua   hiz   n    lat  \"},{\"010\":{\"subfields\":[{\"a\":\"   55001156/M \"}],\"ind1\":\"\",\"ind2\":\" \"}},{\"035\":" +
    "{\"subfields\":[{\"a\":\"(OCoLC)63611770\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"035\":{\"subfields\":[{\"a\":\"393893\"}],\"ind1\":\" \",\"ind2\":\" \"}}," +
    "{\"040\":{\"subfields\":[{\"c\":\"UPB\"},{\"d\":\"UPB\"},{\"d\":\"NIC\"},{\"d\":\"NIC\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"041\":{\"subfields\":[{\"a\":\"latitager\"}," +
    "{\"g\":\"ger\"}],\"ind1\":\"0\",\"ind2\":\" \"}},{\"045\":{\"subfields\":[{\"a\":\"v6v9\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"047\":{\"subfields\":[{\"a\":\"cn\"}," +
    "{\"a\":\"ct\"},{\"a\":\"co\"},{\"a\":\"df\"},{\"a\":\"dv\"},{\"a\":\"ft\"},{\"a\":\"fg\"},{\"a\":\"ms\"},{\"a\":\"mi\"},{\"a\":\"nc\"},{\"a\":\"op\"},{\"a\":\"ov\"}," +
    "{\"a\":\"rq\"},{\"a\":\"sn\"},{\"a\":\"su\"},{\"a\":\"sy\"},{\"a\":\"vr\"},{\"a\":\"zz\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"050\":{\"subfields\":[{\"a\":\"M3\"},{\"b\":\".M896\"}]," +
    "\"ind1\":\"0\",\"ind2\":\" \"}},{\"100\":{\"subfields\":[{\"a\":\"Mozart, Wolfgang Amadeus,\"},{\"d\":\"1756-1791.\"}],\"ind1\":\"1\",\"ind2\":\" \"}},{\"240\":{\"subfields\":" +
    "[{\"a\":\"Works\"}],\"ind1\":\"1\",\"ind2\":\"0\"}},{\"245\":{\"subfields\":[{\"a\":\"Neue Ausgabe sa\\u0308mtlicher Werke,\"},{\"b\":\"in Verbindung mit den Mozartsta\\u0308dten," +
    " Augsburg, Salzburg und Wien.\"},{\"c\":\"Hrsg. von der Internationalen Stiftung Mozarteum, Salzburg.\"}],\"ind1\":\"1\",\"ind2\":\"0\"}},{\"246\":{\"subfields\":[{\"a\":" +
    "\"Neue Mozart-Ausgabe\"}],\"ind1\":\"3\",\"ind2\":\"3\"}},{\"260\":{\"subfields\":[{\"a\":\"Kassel,\"},{\"b\":\"Ba\\u0308renreiter,\"},{\"c\":\"c1955-\"}],\"ind1\":\" \",\"ind2\":\" \"}}," +
    "{\"300\":{\"subfields\":[{\"a\":\"v.\"},{\"b\":\"facsims.\"},{\"c\":\"33 cm.\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"505\":{\"subfields\":[{\"a\":\"Ser. I. Geistliche Gesangswerke -- Ser. " +
    "II. Opern -- Ser. III. Lieder, mehrstimmige Gesa\\u0308nge, Kanons -- Ser. IV. Orchesterwerke -- Ser. V. Konzerte -- Ser. VI. Kirchensonaten -- Ser. VII. Ensemblemusik fu\\u0308r " +
    "gro\\u0308ssere Solobesetzungen -- Ser. VIII. Kammermusik -- Ser. IX. Klaviermusik -- Ser. X. Supplement.\"}],\"ind1\":\"0\",\"ind2\":\" \"}},{\"650\":{\"subfields\":[{\"a\":\"Vocal music\"}]," +
    "\"ind1\":\" \",\"ind2\":\"0\"}},{\"650\":{\"subfields\":[{\"a\":\"Instrumental music\"}],\"ind1\":\" \",\"ind2\":\"0\"}},{\"650\":{\"subfields\":[{\"a\":\"Instrumental music\"},{\"2\":\"fast\"},{\"0\"" +
    ":\"(OCoLC)fst00974414\"}],\"ind1\":\" \",\"ind2\":\"7\"}},{\"650\":{\"subfields\":[{\"a\":\"Vocal music\"},{\"2\":\"fast\"},{\"0\":\"(OCoLC)fst01168379\"}],\"ind1\":\" \",\"ind2\":\"7\"}},{\"902\":{\"subfields\"" +
    ":[{\"a\":\"pfnd\"},{\"b\":\"Austin Music\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"905\":{\"subfields\":[{\"a\":\"19980728120000.0\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"948\":{\"subfields\":" +
    "[{\"a\":\"20100622\"},{\"b\":\"s\"},{\"d\":\"lap11\"},{\"e\":\"lts\"},{\"x\":\"ToAddCatStat\"}],\"ind1\":\"1\",\"ind2\":\" \"}},{\"948\":{\"subfields\":[{\"a\":\"20110818\"},{\"b\":\"r\"}," +
    "{\"d\":\"np55\"},{\"e\":\"lts\"}],\"ind1\":\"0\",\"ind2\":\" \"}},{\"948\":{\"subfields\":[{\"a\":\"20130128\"},{\"b\":\"m\"},{\"d\":\"bmt1\"},{\"e\":\"lts\"}],\"ind1\":\"2\",\"ind2\":\" \"}}," +
    "{\"948\":{\"subfields\":[{\"a\":\"20141106\"},{\"b\":\"m\"},{\"d\":\"batch\"},{\"e\":\"lts\"},{\"x\":\"addfast\"}],\"ind1\":\"2\",\"ind2\":\"\"}}]}\n";
  private static final String RAW_INCORRECT_RECORD = "01240cas a2200397   45000010007000000050017000070080";
  private static final String JSON_INCORRECT_RECORD = "{\"leader\":\"01542ccm a2200361   4500\",";
  private static final String EMPTY_RECORD = " ";
  private static final String NULL_RECORD = null;

  @Test
  public void parseRawRecord(TestContext testContext) {
    RecordParser parser = RecordParserBuilder.buildParser(RecordFormat.MARC, RAW_MARC_RECORD);
    ParsedResult result = parser.parseRecord(RAW_MARC_RECORD);
    testContext.assertFalse(result.isHasError());
    testContext.assertNotNull(result.getParsedRecord());
    testContext.assertNotEquals(result.getParsedRecord().encode(), "");
  }

  @Test
  public void parseRawErrorSource(TestContext testContext) {
    RecordParser parser = RecordParserBuilder.buildParser(RecordFormat.MARC, RAW_MARC_RECORD);
    ParsedResult result = parser.parseRecord(RAW_INCORRECT_RECORD);
    testContext.assertTrue(result.isHasError());
    testContext.assertNotNull(result.getErrors());
    testContext.assertNotEquals(result.getErrors().encode(), "");
  }

  @Test
  public void parsEmptySource(TestContext testContext) {
    RecordParser parser = RecordParserBuilder.buildParser(RecordFormat.MARC, RAW_MARC_RECORD);
    ParsedResult result = parser.parseRecord(EMPTY_RECORD);
    testContext.assertTrue(result.isHasError());
    testContext.assertNotNull(result.getErrors());
    testContext.assertNotEquals(result.getErrors().encode(), "");
  }

  @Test
  public void parsNullSource(TestContext testContext) {
    RecordParser parser = RecordParserBuilder.buildParser(RecordFormat.MARC, RAW_MARC_RECORD);
    ParsedResult result = parser.parseRecord(NULL_RECORD);
    testContext.assertTrue(result.isHasError());
    testContext.assertNotNull(result.getErrors());
    testContext.assertNotEquals(result.getErrors().encode(), "");
  }

  @Test
  public void parseJsonRecord(TestContext testContext) {
    RecordParser parser = RecordParserBuilder.buildParser(RecordFormat.MARC, JSON_MARC_RECORD);
    ParsedResult result = parser.parseRecord(JSON_MARC_RECORD);
    testContext.assertFalse(result.isHasError());
    testContext.assertNotNull(result.getParsedRecord());
    testContext.assertNotEquals(result.getParsedRecord().encode(), "");
  }

  @Test
  public void parseJsonErrorSource(TestContext testContext) {
    RecordParser parser = RecordParserBuilder.buildParser(RecordFormat.MARC, JSON_MARC_RECORD);
    ParsedResult result = parser.parseRecord(JSON_INCORRECT_RECORD);
    testContext.assertTrue(result.isHasError());
    testContext.assertNotNull(result.getErrors());
    testContext.assertNotEquals(result.getErrors().encode(), "");
  }

  @Test
  public void parseJsonEmptySource(TestContext testContext) {
    RecordParser parser = RecordParserBuilder.buildParser(RecordFormat.MARC, JSON_MARC_RECORD);
    ParsedResult result = parser.parseRecord(EMPTY_RECORD);
    testContext.assertTrue(result.isHasError());
    testContext.assertNotNull(result.getErrors());
    testContext.assertNotEquals(result.getErrors().encode(), "");
  }

  @Test
  public void parseNullSource(TestContext testContext) {
    RecordParser parser = RecordParserBuilder.buildParser(RecordFormat.MARC, JSON_MARC_RECORD);
    ParsedResult result = parser.parseRecord(NULL_RECORD);
    testContext.assertTrue(result.isHasError());
    testContext.assertNotNull(result.getErrors());
    testContext.assertNotEquals(result.getErrors().encode(), "");
  }

}
