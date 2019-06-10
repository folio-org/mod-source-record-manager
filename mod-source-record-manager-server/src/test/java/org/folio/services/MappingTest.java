package org.folio.services;

import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.folio.rest.jaxrs.model.Instance;
import org.folio.services.mappers.RecordToInstanceMapper;
import org.folio.services.mappers.RecordToInstanceMapperBuilder;
import org.folio.services.parsers.RecordFormat;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(VertxUnitRunner.class)
public class MappingTest {

  private RecordToInstanceMapper mapper = RecordToInstanceMapperBuilder.buildMapper(RecordFormat.MARC);
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

  @Test
  public void testMarcToInstance() {
    Instance instance = mapper.mapRecord(new JsonObject(JSON_MARC_RECORD));
    System.out.println(instance.toString());
  }
}
