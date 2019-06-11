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
  private static final String JSON_MARC_RECORD = "{\n" +
    "        \"fields\": [\n" +
    "          {\n" +
    "            \"001\": \"9928371\"\n" +
    "          },\n" +
    "          {\n" +
    "            \"005\": \"20170607135506.0\"\n" +
    "          },\n" +
    "          {\n" +
    "            \"008\": \"140326s2014    ilu               cneng d\"\n" +
    "          },\n" +
    "          {\n" +
    "            \"040\": {\n" +
    "              \"ind1\": \" \",\n" +
    "              \"ind2\": \" \",\n" +
    "              \"subfields\": [\n" +
    "                {\n" +
    "                  \"a\": \"MYG\"\n" +
    "                },\n" +
    "                {\n" +
    "                  \"b\": \"eng\"\n" +
    "                },\n" +
    "                {\n" +
    "                  \"e\": \"rda\"\n" +
    "                },\n" +
    "                {\n" +
    "                  \"c\": \"MYG\"\n" +
    "                },\n" +
    "                {\n" +
    "                  \"d\": \"MYG\"\n" +
    "                },\n" +
    "                {\n" +
    "                  \"d\": \"CUT\"\n" +
    "                },\n" +
    "                {\n" +
    "                  \"d\": \"OCLCO\"\n" +
    "                },\n" +
    "                {\n" +
    "                  \"d\": \"OCLCF\"\n" +
    "                },\n" +
    "                {\n" +
    "                  \"d\": \"OCLCO\"\n" +
    "                },\n" +
    "                {\n" +
    "                  \"d\": \"NIC\"\n" +
    "                }\n" +
    "              ]\n" +
    "            }\n" +
    "          },\n" +
    "          {\n" +
    "            \"035\": {\n" +
    "              \"ind1\": \" \",\n" +
    "              \"ind2\": \" \",\n" +
    "              \"subfields\": [\n" +
    "                {\n" +
    "                  \"a\": \"(OCoLC)874849566\"\n" +
    "                }\n" +
    "              ]\n" +
    "            }\n" +
    "          },\n" +
    "          {\n" +
    "            \"043\": {\n" +
    "              \"ind1\": \" \",\n" +
    "              \"ind2\": \" \",\n" +
    "              \"subfields\": [\n" +
    "                {\n" +
    "                  \"a\": \"n-us---\"\n" +
    "                }\n" +
    "              ]\n" +
    "            }\n" +
    "          },\n" +
    "          {\n" +
    "            \"050\": {\n" +
    "              \"ind1\": \"1\",\n" +
    "              \"ind2\": \"4\",\n" +
    "              \"subfields\": [\n" +
    "                {\n" +
    "                  \"a\": \"N7433.3\"\n" +
    "                },\n" +
    "                {\n" +
    "                  \"b\": \".B87 2014\"\n" +
    "                }\n" +
    "              ]\n" +
    "            }\n" +
    "          },\n" +
    "          {\n" +
    "            \"100\": {\n" +
    "              \"ind1\": \"1\",\n" +
    "              \"ind2\": \" \",\n" +
    "              \"subfields\": [\n" +
    "                {\n" +
    "                  \"a\": \"Burtonwood, Tom,\"\n" +
    "                },\n" +
    "                {\n" +
    "                  \"d\": \"1974- ,\"\n" +
    "                },\n" +
    "                {\n" +
    "                  \"e\": \"artist.\"\n" +
    "                }\n" +
    "              ]\n" +
    "            }\n" +
    "          },\n" +
    "          {\n" +
    "            \"245\": {\n" +
    "              \"ind1\": \"1\",\n" +
    "              \"ind2\": \"0\",\n" +
    "              \"subfields\": [\n" +
    "                {\n" +
    "                  \"a\": \"Orihon /\"\n" +
    "                },\n" +
    "                {\n" +
    "                  \"c\": \"Tom Burtonwood.\"\n" +
    "                }\n" +
    "              ]\n" +
    "            }\n" +
    "          },\n" +
    "          {\n" +
    "            \"264\": {\n" +
    "              \"ind1\": \" \",\n" +
    "              \"ind2\": \"1\",\n" +
    "              \"subfields\": [\n" +
    "                {\n" +
    "                  \"a\": \"Brooklyn, New York :\"\n" +
    "                },\n" +
    "                {\n" +
    "                  \"b\": \"Booklyn Artists Alliance,\"\n" +
    "                },\n" +
    "                {\n" +
    "                  \"c\": \"2014.\"\n" +
    "                }\n" +
    "              ]\n" +
    "            }\n" +
    "          },\n" +
    "          {\n" +
    "            \"300\": {\n" +
    "              \"ind1\": \" \",\n" +
    "              \"ind2\": \" \",\n" +
    "              \"subfields\": [\n" +
    "                {\n" +
    "                  \"a\": \"1 object :\"\n" +
    "                },\n" +
    "                {\n" +
    "                  \"b\": \"plastic ;\"\n" +
    "                },\n" +
    "                {\n" +
    "                  \"c\": \"13 x 20 x 20 cm +\"\n" +
    "                },\n" +
    "                {\n" +
    "                  \"e\": \"1 USB flash drive.\"\n" +
    "                }\n" +
    "              ]\n" +
    "            }\n" +
    "          },\n" +
    "          {\n" +
    "            \"336\": {\n" +
    "              \"ind1\": \" \",\n" +
    "              \"ind2\": \" \",\n" +
    "              \"subfields\": [\n" +
    "                {\n" +
    "                  \"a\": \"three-dimensional form\"\n" +
    "                },\n" +
    "                {\n" +
    "                  \"b\": \"tdf\"\n" +
    "                },\n" +
    "                {\n" +
    "                  \"2\": \"rdacontent\"\n" +
    "                }\n" +
    "              ]\n" +
    "            }\n" +
    "          },\n" +
    "          {\n" +
    "            \"336\": {\n" +
    "              \"ind1\": \" \",\n" +
    "              \"ind2\": \" \",\n" +
    "              \"subfields\": [\n" +
    "                {\n" +
    "                  \"a\": \"computer dataset\"\n" +
    "                },\n" +
    "                {\n" +
    "                  \"b\": \"cod\"\n" +
    "                },\n" +
    "                {\n" +
    "                  \"2\": \"rdacontent\"\n" +
    "                },\n" +
    "                {\n" +
    "                  \"3\": \"accompanying USB flash drive\"\n" +
    "                }\n" +
    "              ]\n" +
    "            }\n" +
    "          },\n" +
    "          {\n" +
    "            \"337\": {\n" +
    "              \"ind1\": \" \",\n" +
    "              \"ind2\": \" \",\n" +
    "              \"subfields\": [\n" +
    "                {\n" +
    "                  \"a\": \"unmediated\"\n" +
    "                },\n" +
    "                {\n" +
    "                  \"b\": \"n\"\n" +
    "                },\n" +
    "                {\n" +
    "                  \"2\": \"rdamedia\"\n" +
    "                }\n" +
    "              ]\n" +
    "            }\n" +
    "          },\n" +
    "          {\n" +
    "            \"337\": {\n" +
    "              \"ind1\": \" \",\n" +
    "              \"ind2\": \" \",\n" +
    "              \"subfields\": [\n" +
    "                {\n" +
    "                  \"a\": \"computer\"\n" +
    "                },\n" +
    "                {\n" +
    "                  \"b\": \"c\"\n" +
    "                },\n" +
    "                {\n" +
    "                  \"2\": \"rdamedia\"\n" +
    "                },\n" +
    "                {\n" +
    "                  \"3\": \"accompanying USB flash drive\"\n" +
    "                }\n" +
    "              ]\n" +
    "            }\n" +
    "          },\n" +
    "          {\n" +
    "            \"338\": {\n" +
    "              \"ind1\": \" \",\n" +
    "              \"ind2\": \" \",\n" +
    "              \"subfields\": [\n" +
    "                {\n" +
    "                  \"a\": \"object\"\n" +
    "                },\n" +
    "                {\n" +
    "                  \"b\": \"nr\"\n" +
    "                },\n" +
    "                {\n" +
    "                  \"2\": \"rdacarrier\"\n" +
    "                }\n" +
    "              ]\n" +
    "            }\n" +
    "          },\n" +
    "          {\n" +
    "            \"338\": {\n" +
    "              \"ind1\": \" \",\n" +
    "              \"ind2\": \" \",\n" +
    "              \"subfields\": [\n" +
    "                {\n" +
    "                  \"a\": \"other\"\n" +
    "                },\n" +
    "                {\n" +
    "                  \"b\": \"cz\"\n" +
    "                },\n" +
    "                {\n" +
    "                  \"2\": \"rdacarrier\"\n" +
    "                },\n" +
    "                {\n" +
    "                  \"3\": \"accompanying USB flash drive\"\n" +
    "                }\n" +
    "              ]\n" +
    "            }\n" +
    "          },\n" +
    "          {\n" +
    "            \"500\": {\n" +
    "              \"ind1\": \" \",\n" +
    "              \"ind2\": \" \",\n" +
    "              \"subfields\": [\n" +
    "                {\n" +
    "                  \"a\": \"\\\"Released under the Creative Commons Attribution-ShareAlike 3.0 unported (CC BY-SA 3.0) license\\\"--First leaf.\"\n" +
    "                }\n" +
    "              ]\n" +
    "            }\n" +
    "          },\n" +
    "          {\n" +
    "            \"505\": {\n" +
    "              \"ind1\": \"0\",\n" +
    "              \"ind2\": \"0\",\n" +
    "              \"subfields\": [\n" +
    "                {\n" +
    "                  \"t\": \"Head of an ogre /\"\n" +
    "                },\n" +
    "                {\n" +
    "                  \"r\": \"by Tom Burtonwood --\"\n" +
    "                },\n" +
    "                {\n" +
    "                  \"t\": \"Boddhisattva /\"\n" +
    "                },\n" +
    "                {\n" +
    "                  \"r\": \"by Jason Bakutis --\"\n" +
    "                },\n" +
    "                {\n" +
    "                  \"t\": \"Olmec colossal head /\"\n" +
    "                },\n" +
    "                {\n" +
    "                  \"r\": \"by Pretty Small Things --\"\n" +
    "                },\n" +
    "                {\n" +
    "                  \"t\": \"Mayan head /\"\n" +
    "                },\n" +
    "                {\n" +
    "                  \"r\": \"by Tom Burtonwood --\"\n" +
    "                },\n" +
    "                {\n" +
    "                  \"t\": \"Torso of an emperor, Roman Imperial /\"\n" +
    "                },\n" +
    "                {\n" +
    "                  \"r\": \"by AMinimal Studio --\"\n" +
    "                },\n" +
    "                {\n" +
    "                  \"t\": \"Art Institute of Chicago lion /\"\n" +
    "                },\n" +
    "                {\n" +
    "                  \"r\": \"by Tom Burtonwood.\"\n" +
    "                }\n" +
    "              ]\n" +
    "            }\n" +
    "          },\n" +
    "          {\n" +
    "            \"520\": {\n" +
    "              \"ind1\": \" \",\n" +
    "              \"ind2\": \" \",\n" +
    "              \"subfields\": [\n" +
    "                {\n" +
    "                  \"a\": \"A 3D-printed, book-like object consisting of eight leaves, with six relief illustrations, each created using photogrammetric scans of a sculpture. Leaves are connected by hinges and folded accordion style. The accompanying USB flash drive contains all data files used to create the object with a 3D printer. Six of the leaves feature scans of a sculpture found at the Art Institute of Chicago and the Metropolitan Museum of Art in New York.\"\n" +
    "                }\n" +
    "              ]\n" +
    "            }\n" +
    "          },\n" +
    "          {\n" +
    "            \"588\": {\n" +
    "              \"ind1\": \" \",\n" +
    "              \"ind2\": \" \",\n" +
    "              \"subfields\": [\n" +
    "                {\n" +
    "                  \"a\": \"Title from publisher's website (viewed March 26, 2014).\"\n" +
    "                }\n" +
    "              ]\n" +
    "            }\n" +
    "          },\n" +
    "          {\n" +
    "            \"650\": {\n" +
    "              \"ind1\": \" \",\n" +
    "              \"ind2\": \"0\",\n" +
    "              \"subfields\": [\n" +
    "                {\n" +
    "                  \"a\": \"Artists' books\"\n" +
    "                },\n" +
    "                {\n" +
    "                  \"z\": \"United States.\"\n" +
    "                }\n" +
    "              ]\n" +
    "            }\n" +
    "          },\n" +
    "          {\n" +
    "            \"650\": {\n" +
    "              \"ind1\": \" \",\n" +
    "              \"ind2\": \"0\",\n" +
    "              \"subfields\": [\n" +
    "                {\n" +
    "                  \"a\": \"Three-dimensional printing.\"\n" +
    "                }\n" +
    "              ]\n" +
    "            }\n" +
    "          },\n" +
    "          {\n" +
    "            \"650\": {\n" +
    "              \"ind1\": \" \",\n" +
    "              \"ind2\": \"7\",\n" +
    "              \"subfields\": [\n" +
    "                {\n" +
    "                  \"a\": \"Artists' books.\"\n" +
    "                },\n" +
    "                {\n" +
    "                  \"2\": \"fast\"\n" +
    "                },\n" +
    "                {\n" +
    "                  \"0\": \"(OCoLC)fst00817660\"\n" +
    "                }\n" +
    "              ]\n" +
    "            }\n" +
    "          },\n" +
    "          {\n" +
    "            \"650\": {\n" +
    "              \"ind1\": \" \",\n" +
    "              \"ind2\": \"7\",\n" +
    "              \"subfields\": [\n" +
    "                {\n" +
    "                  \"a\": \"Three-dimensional printing.\"\n" +
    "                },\n" +
    "                {\n" +
    "                  \"2\": \"fast\"\n" +
    "                },\n" +
    "                {\n" +
    "                  \"0\": \"(OCoLC)fst01748862\"\n" +
    "                }\n" +
    "              ]\n" +
    "            }\n" +
    "          },\n" +
    "          {\n" +
    "            \"651\": {\n" +
    "              \"ind1\": \" \",\n" +
    "              \"ind2\": \"7\",\n" +
    "              \"subfields\": [\n" +
    "                {\n" +
    "                  \"a\": \"United States.\"\n" +
    "                },\n" +
    "                {\n" +
    "                  \"2\": \"fast\"\n" +
    "                },\n" +
    "                {\n" +
    "                  \"0\": \"(OCoLC)fst01204155\"\n" +
    "                }\n" +
    "              ]\n" +
    "            }\n" +
    "          },\n" +
    "          {\n" +
    "            \"655\": {\n" +
    "              \"ind1\": \" \",\n" +
    "              \"ind2\": \"7\",\n" +
    "              \"subfields\": [\n" +
    "                {\n" +
    "                  \"a\": \"Artists' books.\"\n" +
    "                },\n" +
    "                {\n" +
    "                  \"2\": \"aat\"\n" +
    "                }\n" +
    "              ]\n" +
    "            }\n" +
    "          },\n" +
    "          {\n" +
    "            \"700\": {\n" +
    "              \"ind1\": \"1\",\n" +
    "              \"ind2\": \" \",\n" +
    "              \"subfields\": [\n" +
    "                {\n" +
    "                  \"a\": \"Bakutis, Jason,\"\n" +
    "                },\n" +
    "                {\n" +
    "                  \"d\": \"1969- ,\"\n" +
    "                },\n" +
    "                {\n" +
    "                  \"e\": \"artist.\"\n" +
    "                }\n" +
    "              ]\n" +
    "            }\n" +
    "          },\n" +
    "          {\n" +
    "            \"710\": {\n" +
    "              \"ind1\": \"2\",\n" +
    "              \"ind2\": \" \",\n" +
    "              \"subfields\": [\n" +
    "                {\n" +
    "                  \"a\": \"AMinimal Studio.\"\n" +
    "                }\n" +
    "              ]\n" +
    "            }\n" +
    "          },\n" +
    "          {\n" +
    "            \"710\": {\n" +
    "              \"ind1\": \"2\",\n" +
    "              \"ind2\": \" \",\n" +
    "              \"subfields\": [\n" +
    "                {\n" +
    "                  \"a\": \"Booklyn Artists Alliance,\"\n" +
    "                },\n" +
    "                {\n" +
    "                  \"e\": \"publisher.\"\n" +
    "                }\n" +
    "              ]\n" +
    "            }\n" +
    "          },\n" +
    "          {\n" +
    "            \"710\": {\n" +
    "              \"ind1\": \"2\",\n" +
    "              \"ind2\": \" \",\n" +
    "              \"subfields\": [\n" +
    "                {\n" +
    "                  \"a\": \"Pretty Small Things.\"\n" +
    "                }\n" +
    "              ]\n" +
    "            }\n" +
    "          },\n" +
    "          {\n" +
    "            \"856\": {\n" +
    "              \"ind1\": \"4\",\n" +
    "              \"ind2\": \"1\",\n" +
    "              \"subfields\": [\n" +
    "                {\n" +
    "                  \"u\": \"http://www.thingiverse.com/thing:110411\"\n" +
    "                },\n" +
    "                {\n" +
    "                  \"z\": \"3D printer files also available online\"\n" +
    "                }\n" +
    "              ]\n" +
    "            }\n" +
    "          },\n" +
    "          {\n" +
    "            \"856\": {\n" +
    "              \"ind1\": \"4\",\n" +
    "              \"ind2\": \"2\",\n" +
    "              \"subfields\": [\n" +
    "                {\n" +
    "                  \"u\": \"http://booklyn.org/archive/index.php/Detail/Object/Show/object_id/1513\"\n" +
    "                },\n" +
    "                {\n" +
    "                  \"z\": \"Publisher's description\"\n" +
    "                }\n" +
    "              ]\n" +
    "            }\n" +
    "          },\n" +
    "          {\n" +
    "            \"948\": {\n" +
    "              \"ind1\": \"1\",\n" +
    "              \"ind2\": \" \",\n" +
    "              \"subfields\": [\n" +
    "                {\n" +
    "                  \"a\": \"20170607\"\n" +
    "                },\n" +
    "                {\n" +
    "                  \"b\": \"c\"\n" +
    "                },\n" +
    "                {\n" +
    "                  \"d\": \"mnr1\"\n" +
    "                },\n" +
    "                {\n" +
    "                  \"e\": \"lts\"\n" +
    "                }\n" +
    "              ]\n" +
    "            }\n" +
    "          },\n" +
    "          {\n" +
    "            \"999\": {\n" +
    "              \"ind1\": \"f\",\n" +
    "              \"ind2\": \"f\",\n" +
    "              \"subfields\": [\n" +
    "                {\n" +
    "                  \"s\": \"85b5ab23-7a49-4c65-8adf-394c0fcad97c\"\n" +
    "                },\n" +
    "                {\n" +
    "                  \"i\": \"ddda75d7-1364-4b46-84c8-98892327b572\"\n" +
    "                }\n" +
    "              ]\n" +
    "            }\n" +
    "          }\n" +
    "        ],\n" +
    "        \"leader\": \"02566crm a2200433Ii 4500\"\n" +
    "      }";

  @Test
  public void testMarcToInstance() {
    Instance instance = mapper.mapRecord(new JsonObject(JSON_MARC_RECORD));
    System.out.println(JsonObject.mapFrom(instance).toString());
  }
}
