package org.folio.rest.impl.changeManager;

import static com.github.tomakehurst.wiremock.client.WireMock.created;
import static com.github.tomakehurst.wiremock.client.WireMock.deleteRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.serverError;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_INCOMING_MARC_BIB_RECORD_PARSED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_ERROR;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_MARC_FOR_UPDATE_RECEIVED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_RAW_RECORDS_CHUNK_PARSED;
import static org.folio.rest.jaxrs.model.ProfileType.ACTION_PROFILE;
import static org.folio.rest.jaxrs.model.ProfileType.JOB_PROFILE;
import static org.folio.rest.jaxrs.model.ProfileType.MATCH_PROFILE;
import static org.folio.rest.jaxrs.model.StatusDto.Status.CANCELLED;
import static org.folio.rest.jaxrs.model.StatusDto.Status.COMMITTED;
import static org.folio.rest.jaxrs.model.StatusDto.Status.ERROR;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertNotNull;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.matching.RegexPattern;
import com.github.tomakehurst.wiremock.matching.UrlPathPattern;
import io.restassured.RestAssured;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import net.mguenther.kafka.junit.ObserveKeyValues;
import org.apache.http.HttpStatus;
import org.folio.MatchProfile;
import org.folio.TestUtil;
import org.folio.dao.JournalRecordDao;
import org.folio.dao.JournalRecordDaoImpl;
import org.folio.dao.util.PostgresClientFactory;
import org.folio.rest.impl.AbstractRestTest;
import org.folio.rest.jaxrs.model.ActionProfile;
import org.folio.rest.jaxrs.model.DataImportEventPayload;
import org.folio.rest.jaxrs.model.DeleteJobExecutionsReq;
import org.folio.rest.jaxrs.model.DeleteJobExecutionsResp;
import org.folio.rest.jaxrs.model.Event;
import org.folio.rest.jaxrs.model.File;
import org.folio.rest.jaxrs.model.InitJobExecutionsRqDto;
import org.folio.rest.jaxrs.model.InitJobExecutionsRsDto;
import org.folio.rest.jaxrs.model.InitialRecord;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JobProfile;
import org.folio.rest.jaxrs.model.JobProfileInfo;
import org.folio.rest.jaxrs.model.JobProfileInfo.DataType;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.folio.rest.jaxrs.model.Progress;
import org.folio.rest.jaxrs.model.RawRecordsDto;
import org.folio.rest.jaxrs.model.RecordCollection;
import org.folio.rest.jaxrs.model.RecordsMetadata;
import org.folio.rest.jaxrs.model.RunBy;
import org.folio.rest.jaxrs.model.StatusDto;
import org.folio.services.Status;
import org.hamcrest.MatcherAssert;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;

/**
 * REST tests for ChangeManager to manager JobExecution entities initialization
 */
@RunWith(VertxUnitRunner.class)
public class ChangeManagerAPITest extends AbstractRestTest {

  private static final String RECORDS_PATH = "/records";
  private static final String CHILDREN_PATH = "/children";
  private static final String STATUS_PATH = "/status";
  private static final String JOB_PROFILE_PATH = "/jobProfile";
  private static final String RAW_DTO_PATH = "src/test/resources/org/folio/rest/rawRecordsDto.json";

  private static final String CORRECT_RAW_RECORD_1 =
    "01240cas a2200397   450000100070000000500170000700800410002401000170006502200140008203500260009603500220012203500110014403500190015504000440017405000150021808200110023322200420024424500430028626000470032926500380037630000150041431000220042932100250045136200230047657000290049965000330052865000450056165500420060670000450064885300180069386300230071190200160073490500210075094800370077195000340080836683220141106221425.0750907c19509999enkqr p       0   a0eng d  a   58020553   a0022-0469  a(CStRLIN)NYCX1604275S  a(NIC)notisABP6388  a366832  a(OCoLC)1604275  dCtYdMBTIdCtYdMBTIdNICdCStRLINdNIC0 aBR140b.J6  a270.0504aThe Journal of ecclesiastical history04aThe Journal of ecclesiastical history.  aLondon,bCambridge University Press [etc.]  a32 East 57th St., New York, 10022  av.b25 cm.  aQuarterly,b1970-  aSemiannual,b1950-690 av. 1-   Apr. 1950-  aEditor:   C. W. Dugmore. 0aChurch historyxPeriodicals. 7aChurch history2fast0(OCoLC)fst00860740 7aPeriodicals2fast0(OCoLC)fst014116411 aDugmore, C. W.q(Clifford William),eed.0381av.i(year)4081a1-49i1950-1998  apfndbLintz  a19890510120000.02 a20141106bmdbatcheltsxaddfast  lOLINaBR140b.J86h01/01/01 N01542ccm a2200361   ";
  private static final String CORRECT_RAW_RECORD_2 =
    "01314nam  22003851a 4500001001100000003000800011005001700019006001800036007001500054008004100069020003200110020003500142040002100177050002000198082001500218100002000233245008900253250001200342260004900354300002300403490002400426500002400450504006200474505009200536650003200628650001400660700002500674710001400699776004000713830001800753856009400771935001500865980003400880981001400914\u001Eybp7406411\u001ENhCcYBP\u001E20120404100627.6\u001Em||||||||d|||||||\u001Ecr||n|||||||||\u001E120329s2011    sz a    ob    001 0 eng d\u001E  \u001Fa2940447241 (electronic bk.)\u001E  \u001Fa9782940447244 (electronic bk.)\u001E  \u001FaNhCcYBP\u001FcNhCcYBP\u001E 4\u001FaZ246\u001Fb.A43 2011\u001E04\u001Fa686.22\u001F222\u001E1 \u001FaAmbrose, Gavin.\u001E14\u001FaThe fundamentals of typography\u001Fh[electronic resource] /\u001FcGavin Ambrose, Paul Harris.\u001E  \u001Fa2nd ed.\u001E  \u001FaLausanne ;\u001FaWorthing :\u001FbAVA Academia,\u001Fc2011.\u001E  \u001Fa1 online resource.\u001E1 \u001FaAVA Academia series\u001E  \u001FaPrevious ed.: 2006.\u001E  \u001FaIncludes bibliographical references (p. [200]) and index.\u001E0 \u001FaType and language -- A few basics -- Letterforms -- Words and paragraphs -- Using type.\u001E 0\u001FaGraphic design (Typography)\u001E 0\u001FaPrinting.\u001E1 \u001FaHarris, Paul,\u001Fd1971-\u001E2 \u001FaEBSCOhost\u001E  \u001FcOriginal\u001Fz9782940411764\u001Fz294041176X\u001E 0\u001FaAVA academia.\u001E40\u001Fuhttp://search.ebscohost.com/login.aspx?direct=true&scope=site&db=nlebk&db=nlabk&AN=430135\u001E  \u001Fa.o13465259\u001E  \u001Fa130307\u001Fb7107\u001Fe7107\u001Ff243965\u001Fg1\u001E  \u001FbOM\u001Fcnlnet\u001E\u001D\n";
  private static final String CORRECT_RAW_RECORD_3 =
    "03401nam  22004091i 4500001001200000003000800012005001700020006001800037007001500055008004100070020003200111020003500143020004300178020004000221040002100261050002700282082001900309245016200328300002300490500004700513504005100560505178700611588004702398650003702445650004502482650002902527650003602556700005502592700006102647710002302708730001902731776005902750856011902809935001502928980003402943981001402977\u001Eybp10134220\u001ENhCcYBP\u001E20130220102526.4\u001Em||||||||d|||||||\u001Ecr||n|||||||||\u001E130220s2013    ncu     ob    001 0 eng d\u001E  \u001Fa1476601852 (electronic bk.)\u001E  \u001Fa9781476601854 (electronic bk.)\u001E  \u001Fz9780786471140 (softcover : alk. paper)\u001E  \u001Fz078647114X (softcover : alk. paper)\u001E  \u001FaNhCcYBP\u001FcNhCcYBP\u001E 4\u001FaPN1995.9.V46\u001FbG37 2013\u001E04\u001Fa791.43/656\u001F223\u001E00\u001FaGame on, Hollywood!\u001Fh[electronic resource] :\u001Fbessays on the intersection of video games and cinema /\u001Fcedited by Gretchen Papazian and Joseph Michael Sommers.\u001E  \u001Fa1 online resource.\u001E  \u001FaDescription based on print version record.\u001E  \u001FaIncludes bibliographical references and index.\u001E0 \u001FaIntroduction: manifest narrativity-video games, movies, and art and adaptation / Gretchen Papazian and Joseph Michael Sommers -- The rules of engagement: watching, playing and other narrative processes. Playing the Buffyverse, playing the gothic: genre, gender and cross-media interactivity in Buffy the vampire slayer: chaos bleeds / Katrin Althans -- Dead eye: the spectacle of torture porn in Dead rising / Deborah Mellamphy -- Playing (with) the western: classical Hollywood genres in modern video games / Jason W. Buel -- Game-to-film adaptation and how Prince of Persia: the sands of time negotiates the difference between player and audience / Ben S. Bunting, Jr -- Translation between forms of interactivity: how to build the better adaptation / Marcus Schulzke -- The terms of the tale: time, place and other ideologically constructed conditions. -- Playing (in) the city: the warriors and images of urban disorder / Aubrey Anable -- When did Dante become a scythe-wielding badass? modeling adaption and shifting gender convention in Dante's Inferno / Denise A. Ayo -- \"Some of this happened to the other fellow\": remaking Goldeneye with Daniel Craig / David McGowan -- Zombie stripper geishas in the new global economy: racism and sexism in video games / Stewart Chang -- Stories, stories everywhere (and nowhere just the same): transmedia texts. \"My name is Alan Wake. I'm a writer.\": crafting narrative complexity in the age of transmedia storytelling / Michael Fuchs -- Millions of voices: Star wars, digital games, fictional worlds and franchise canon / Felan Parker -- The hype man as racial stereotype, parody and ghost in Afro samurai / Treaandrea M. Russworm -- Epic nostalgia: narrative play and transmedia storytelling in Disney epic Mickey / Lisa K. Dusenberry.\u001E  \u001FaDescription based on print version record.\u001E 0\u001FaMotion pictures and video games.\u001E 0\u001FaFilm adaptations\u001FxHistory and criticism.\u001E 0\u001FaVideo games\u001FxAuthorship.\u001E 0\u001FaConvergence (Telecommunication)\u001E1 \u001FaPapazian, Gretchen,\u001Fd1968-\u001Feeditor of compilation.\u001E1 \u001FaSommers, Joseph Michael,\u001Fd1976-\u001Feeditor of  compilation.\u001E2 \u001FaEbooks Corporation\u001E0 \u001FaEbook Library.\u001E08\u001FcOriginal\u001Fz9780786471140\u001Fz078647114X\u001Fw(DLC)  2012051432\u001E40\u001FzConnect to e-book on Ebook Library\u001Fuhttp://qut.eblib.com.au.AU/EBLWeb/patron/?target=patron&extendedid=P_1126326_0\u001E  \u001Fa.o13465405\u001E  \u001Fa130307\u001Fb6000\u001Fe6000\u001Ff243967\u001Fg1\u001E  \u001FbOM\u001Fcnlnet\u001E\u001D";
  private static final String CORRECT_MARC_HOLDINGS_RAW_RECORD =
    "00182cx  a22000851  4500001000900000004000800009005001700017008003300034852002900067\u001E10245123\u001E9928371\u001E20170607135730.0\u001E1706072u    8   4001uu   0901128\u001E0 \u001Fbfine\u001FhN7433.3\u001Fi.B87 2014\u001E\u001D";
  private static final String RAW_RECORD_WITH_999_ff_s_SUBFIELD =
    "00948nam a2200241 a 4500001000800000003000400008005001700012008004100029035002100070035002000091040002300111041001300134100002300147245007900170260005800249300002400307440007100331650003600402650005500438650006900493655006500562999007900627\u001E1007048\u001EICU\u001E19950912000000.0\u001E891218s1983    wyu      d    00010 eng d\u001E  \u001Fa(ICU)BID12424550\u001E  \u001Fa(OCoLC)16105467\u001E  \u001FaPAU\u001FcPAU\u001Fdm/c\u001FdICU\u001E0 \u001Faeng\u001Faarp\u001E1 \u001FaSalzmann, Zdeněk\u001E10\u001FaDictionary of contemporary Arapaho usage /\u001Fccompiled by Zdeněk Salzmann.\u001E0 \u001FaWind River, Wyoming :\u001FbWind River Reservation,\u001Fc1983.\u001E  \u001Fav, 231 p. ;\u001Fc28 cm.\u001E 0\u001FaArapaho language and culture instructional materials series\u001Fvno. 4\u001E 0\u001FaArapaho language\u001FxDictionaries.\u001E 0\u001FaIndians of North America\u001FxLanguages\u001FxDictionaries.\u001E 7\u001FaArapaho language.\u001F2fast\u001F0http://id.worldcat.org/fast/fst00812722\u001E 7\u001FaDictionaries.\u001F2fast\u001F0http://id.worldcat.org/fast/fst01423826\u001Eff\u001Fie27a5374-0857-462e-ac84-fb4795229c7a\u001Fse27a5374-0857-462e-ac84-fb4795229c7a\u001E\u001D";
  private static final String MARC_AUTHORITY_RAW_RECORD_WITH_999_s_SUBFIELD =
    "01107cz  a2200253n  4500001000800000005001700008008004100025010001700066035002300083035002100106040004300127100002200170375000900192377000800201400002400209400003700233400003000270400004400300667004700344670006800391670009400459670022100553999007900774\u001E1000649\u001E20171119085041.0\u001E850103n| azannaabn          |b aaa      \u001E  \u001Fan  84234537 \u001E  \u001Fa(OCoLC)oca01249182\u001E  \u001Fa(DLC)n  84234537\u001E  \u001FaDLC\u001Fbeng\u001Ferda\u001FcDLC\u001FdDLC\u001FdCoU\u001FdDLC\u001FdInU\u001E1 \u001FaEimermacher, Karl\u001E  \u001Famale\u001E  \u001Fager\u001E1 \u001FaAĭmermakher, Karl\u001E1 \u001FaАймермахер, Карл\u001E1 \u001FaAĭmermakher, K.\u001Fq(Karl)\u001E1 \u001FaАймермахер, К.\u001Fq(Карл)\u001E  \u001FaNon-Latin script references not evaluated.\u001E  \u001FaSidur, V. Vadim Sidur, 1980?:\u001Fbp. 3 of cover (Karl Eimermacher)\u001E  \u001FaV tiskakh ideologii, 1992:\u001Fbt.p. verso (Karla Aĭmermakhera) colophon (K. Aĭmermakher)\u001E  \u001FaGoogle, 01-31-02\u001Fbruhr-uni-bochum.de/lirsk/whowho.htm (Prof. Dr. Dr. hc. Karl Eimermacher; Geb. 1938; Institutsleiter und Landesbeauftragter für die Hochschulkontakte des Landes NRW zu den europäischen U-Staaten)\u001Eff\u001Fs9df3f407-434b-4260-bfe3-3791c607a1cd\u001Fi92e3bde3-d49f-4a25-867b-2fc257bee318\u001E\u001D";
  private static final String RAW_RECORD_WITH_INVALID_LEADER =
    "01342nhm a2200289 a 4500005001700000008004100017035002000058040001300078041000800091043001200099245003000111260005600141300002100197336008000218337007100298338006900369350001500438500007900453530004900532650008200581651006500663650007900728650006300807651006600870856009600936035002001032\u001E20020829132600.0\u001E850404m19839999ctu     ab    00000dmul d\u001E  \u001Fa(ICU)BID8683551\u001E  \u001FaICU\u001FcICU\u001E0 \u001Famul\u001E  \u001Fan-us---\u001E04\u001FaThe Immigrant in America.\u001E0 \u001FaWoodbridge, Conn. :\u001FbResearch Publications,\u001Fc[1983-\u001E  \u001Fareels. ;\u001Fc35 mm.\u001E  \u001Faunspecified\u001Fbzzz\u001F2rdacontent\u001F0http://id.loc.gov/vocabulary/contentTypes/zzz\u001E  \u001Faunmediated\u001Fbn\u001F2rdamedia\u001F0http://id.loc.gov/vocabulary/mediaTypes/n\u001E  \u001Favolume\u001Fbnc\u001F2rdacarrier\u001F0http://id.loc.gov/vocabulary/carriers/nc\u001E  \u001Fa$17,000.00\u001E  \u001FaAccompanying guide to the collection on microfilm has call no. JV6455.I56.\u001E  \u001FaSearch guide also available on the Internet.\u001E 0\u001FaNoncitizens\u001FzUnited States\u001F0http://id.loc.gov/authorities/subjects/sh85003551\u001E 0\u001FaUnited States\u001FxEmigration and immigration\u001FxBio-bibliography.\u001E 7\u001FaEmigration and immigration.\u001F2fast\u001F0http://id.worldcat.org/fast/fst00908690\u001E 7\u001FaImmigrants.\u001F2fast\u001F0http://id.worldcat.org/fast/fst00967712\u001E 7\u001FaUnited States.\u001F2fast\u001F0http://id.worldcat.org/fast/fst01204155\u001E41\u001FzSelect search guide from pull down menu at:\u001Fuhttp://microformguides.gale.com/SearchForm.asp\u001E  \u001Fa(OCoLC)43567633\u001E\u001D";
  private static final String MARC_HOLDINGS_RAW_RECORD_WITH_999_ff_s_SUBFIELD =
    "00476cy  a22001454  4500001000700000004001400007005001700021008003300038014001700071014001400088852009600102866002700198868002600225999007900251\u001E445553\u001Ein00000000278\u001E20171018085818.0\u001E9301234u    8   1   uu          \u001E1 \u001FaABP6388CU001\u001E0 \u001F9000442923\u001E01\u001FbKU/CC/DI/A\u001FhBR140\u001Fi.J86\u001Fxdbe=c\u001FzCurrent issues in Periodicals Room\u001FxCHECK-IN RECORD CREATED\u001E41\u001F80\u001Fav.54-68 (2003-2017)\u001E41\u001F80\u001Fav.1/50 (1950/1999)\u001Eff\u001Fsa88255be-9669-4100-9ad3-25d21827c9bc\u001Fided3fee0-4b2f-41fb-b740-aaf1ffc3e772\u001E\u001D";
  private static final String DEFAULT_INSTANCE_JOB_PROFILE_ID = "e34d7b92-9b83-11eb-a8b3-0242ac130003";
  private static final String DEFAULT_MARC_HOLDINGS_JOB_PROFILE_ID = "80898dee-449f-44dd-9c8e-37d5eb469b1d";

  private static final String DEFAULT_MARC_AUTHORITY_JOB_PROFILE_ID = "6eefa4c6-bbf7-4845-ad82-de7fc5abd0e3";
  private static final String JOB_PROFILE_ID = UUID.randomUUID().toString();

  private Set<JobExecution.SubordinationType> parentTypes = EnumSet.of(
    JobExecution.SubordinationType.PARENT_SINGLE,
    JobExecution.SubordinationType.PARENT_MULTIPLE
  );

  private JobExecution jobExecution = new JobExecution()
    .withId("5105b55a-b9a3-4f76-9402-a5243ea63c95")
    .withHrId(1000)
    .withParentJobId("5105b55a-b9a3-4f76-9402-a5243ea63c95")
    .withSubordinationType(JobExecution.SubordinationType.PARENT_SINGLE)
    .withStatus(JobExecution.Status.NEW)
    .withUiStatus(JobExecution.UiStatus.INITIALIZATION)
    .withSourcePath("importMarc.mrc")
    .withJobProfileInfo(new JobProfileInfo().withId(DEFAULT_INSTANCE_JOB_PROFILE_ID).withName("Marc jobs profile"))
    .withUserId(UUID.randomUUID().toString());

  private RawRecordsDto rawRecordsDto = new RawRecordsDto()
    .withId(UUID.randomUUID().toString())
    .withRecordsMetadata(new RecordsMetadata()
      .withLast(false)
      .withCounter(15)
      .withTotal(15)
      .withContentType(RecordsMetadata.ContentType.MARC_RAW))
    .withInitialRecords(singletonList(new InitialRecord().withRecord(CORRECT_RAW_RECORD_1))
    );

  private final JsonObject userResponse = new JsonObject()
    .put("users",
      new JsonArray().add(new JsonObject()
        .put("username", "diku_admin")))
    .put("totalRecords", 1);

  private RawRecordsDto rawRecordsDto_2;

  {
    try {
      rawRecordsDto_2 = new JsonObject(TestUtil.readFileFromPath(RAW_DTO_PATH)).mapTo(RawRecordsDto.class);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private RawRecordsDto rawRecordsDto_3 = new RawRecordsDto()
    .withId(UUID.randomUUID().toString())
    .withRecordsMetadata(new RecordsMetadata()
      .withLast(false)
      .withCounter(15)
      .withContentType(RecordsMetadata.ContentType.MARC_RAW))
    .withInitialRecords(singletonList(new InitialRecord().withRecord(RAW_RECORD_WITH_999_ff_s_SUBFIELD))
    );

  private RawRecordsDto rawRecordsDto_4 = new RawRecordsDto()
    .withId(UUID.randomUUID().toString())
    .withRecordsMetadata(new RecordsMetadata()
      .withLast(false)
      .withCounter(15)
      .withContentType(RecordsMetadata.ContentType.MARC_RAW))
    .withInitialRecords(singletonList(new InitialRecord().withRecord(RAW_RECORD_WITH_INVALID_LEADER))
    );

  private RawRecordsDto authorityRawRecordsDto = new RawRecordsDto()
    .withId(UUID.randomUUID().toString())
    .withRecordsMetadata(new RecordsMetadata()
      .withLast(false)
      .withCounter(15)
      .withContentType(RecordsMetadata.ContentType.MARC_RAW))
    .withInitialRecords(singletonList(new InitialRecord().withRecord(MARC_AUTHORITY_RAW_RECORD_WITH_999_s_SUBFIELD)));

  private RawRecordsDto marcHoldingsRawRecordDto = new RawRecordsDto()
    .withId(UUID.randomUUID().toString())
    .withRecordsMetadata(new RecordsMetadata()
      .withLast(false)
      .withCounter(15)
      .withContentType(RecordsMetadata.ContentType.MARC_RAW))
    .withInitialRecords(singletonList(new InitialRecord().withRecord(MARC_HOLDINGS_RAW_RECORD_WITH_999_ff_s_SUBFIELD))
    );

  @Spy
  private PostgresClientFactory postgresClientFactory = new PostgresClientFactory(Vertx.vertx());
  @InjectMocks
  private JournalRecordDao journalRecordDao = new JournalRecordDaoImpl();

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    WireMock.stubFor(
      WireMock.get("/data-import-profiles/jobProfiles/" + DEFAULT_INSTANCE_JOB_PROFILE_ID + "?withRelations=false&")
        .willReturn(WireMock.ok().withBody(
          Json.encode(new JobProfile().withId(DEFAULT_INSTANCE_JOB_PROFILE_ID).withName("Default job profile")))));
    WireMock.stubFor(WireMock.get(
        "/data-import-profiles/jobProfiles/" + DEFAULT_MARC_HOLDINGS_JOB_PROFILE_ID + "?withRelations=false&")
      .willReturn(WireMock.ok().withBody(Json.encode(new JobProfile().withId(DEFAULT_MARC_HOLDINGS_JOB_PROFILE_ID)
        .withName("Default - Create Holdings and SRS MARC Holdings")))));
    WireMock.stubFor(WireMock.get("/data-import-profiles/jobProfiles/" + JOB_PROFILE_ID + "?withRelations=false&")
      .willReturn(WireMock.ok()
        .withBody(Json.encode(new JobProfile().withId(JOB_PROFILE_ID).withName("not default job profile")))));
    WireMock.stubFor(WireMock.post("/source-storage/batch/verified-records")
      .willReturn(
        WireMock.ok().withBody(Json.encode(new JsonObject("{\"invalidMarcBibIds\" : [ \"111111\", \"222222\" ]}")))));
    WireMock.stubFor(WireMock.get("/linking-rules/instance-authority")
      .willReturn(WireMock.ok().withBody(Json.encode(emptyList()))));
  }

  @Test
  public void testInitJobExecutionsWith1File() {
    // given
    int expectedJobExecutionsNumber = 1;

    // when
    InitJobExecutionsRsDto response =
      constructAndPostInitJobExecutionRqDto(expectedJobExecutionsNumber);

    // then
    String actualParentJobExecutionId = response.getParentJobExecutionId();
    List<JobExecution> actualJobExecutions = response.getJobExecutions();

    Assert.assertNotNull(actualParentJobExecutionId);
    assertEquals(expectedJobExecutionsNumber, actualJobExecutions.size());

    JobExecution parentSingle = actualJobExecutions.get(0);
    assertEquals(JobExecution.SubordinationType.PARENT_SINGLE, parentSingle.getSubordinationType());
    assertParent(parentSingle);
  }

  @Test
  public void testInitJobExecutionsWithJobProfile() {
    // given
    int expectedJobExecutionsNumber = 1;

    // when
    InitJobExecutionsRqDto requestDto = new InitJobExecutionsRqDto();
    requestDto.setUserId(okapiUserIdHeader);
    requestDto.setSourceType(InitJobExecutionsRqDto.SourceType.ONLINE);
    requestDto.setJobProfileInfo(new JobProfileInfo()
      .withId(DEFAULT_INSTANCE_JOB_PROFILE_ID)
      .withDataType(JobProfileInfo.DataType.MARC)
      .withName("Test Profile"));

    InitJobExecutionsRsDto response = RestAssured.given()
      .spec(spec)
      .body(JsonObject.mapFrom(requestDto).toString())
      .when().log().all()
      .post(JOB_EXECUTION_PATH).body().as(InitJobExecutionsRsDto.class);

    // then
    String actualParentJobExecutionId = response.getParentJobExecutionId();
    List<JobExecution> actualJobExecutions = response.getJobExecutions();

    Assert.assertNotNull(actualParentJobExecutionId);
    assertEquals(expectedJobExecutionsNumber, actualJobExecutions.size());

    JobExecution parentSingle = actualJobExecutions.get(0);
    Assert.assertNotNull(parentSingle);
    assertEquals(JobExecution.SubordinationType.PARENT_SINGLE, parentSingle.getSubordinationType());
    Assert.assertNotNull(parentSingle.getId());
    Assert.assertNotNull(parentSingle.getParentJobId());
    Assert.assertTrue(parentTypes.contains(parentSingle.getSubordinationType()));
    assertEquals(parentSingle.getId(), parentSingle.getParentJobId());
    assertEquals(JobExecution.Status.NEW, parentSingle.getStatus());
    Assert.assertNotNull(parentSingle.getJobProfileInfo());
    Assert.assertNotNull(parentSingle.getRunBy().getFirstName());
    Assert.assertNotNull(parentSingle.getRunBy().getLastName());
  }

  @Test
  public void testInitJobExecutionsWithoutJobProfileAndOnline() {
    // given
    int expectedJobExecutionsNumber = 1;

    // when
    InitJobExecutionsRqDto requestDto = new InitJobExecutionsRqDto();
    requestDto.setUserId(okapiUserIdHeader);
    requestDto.setSourceType(InitJobExecutionsRqDto.SourceType.ONLINE);

    InitJobExecutionsRsDto response = RestAssured.given()
      .spec(spec)
      .body(JsonObject.mapFrom(requestDto).toString())
      .when().log().all()
      .post(JOB_EXECUTION_PATH).body().as(InitJobExecutionsRsDto.class);

    // then
    String actualParentJobExecutionId = response.getParentJobExecutionId();
    List<JobExecution> actualJobExecutions = response.getJobExecutions();

    Assert.assertNotNull(actualParentJobExecutionId);
    assertEquals(expectedJobExecutionsNumber, actualJobExecutions.size());

    JobExecution parentSingle = actualJobExecutions.get(0);
    Assert.assertNotNull(parentSingle);
    assertEquals(JobExecution.SubordinationType.PARENT_SINGLE, parentSingle.getSubordinationType());
    Assert.assertNotNull(parentSingle.getId());
    Assert.assertNotNull(parentSingle.getParentJobId());
    Assert.assertTrue(parentTypes.contains(parentSingle.getSubordinationType()));
    assertEquals(parentSingle.getId(), parentSingle.getParentJobId());
    assertEquals(JobExecution.Status.NEW, parentSingle.getStatus());
    Assert.assertNotNull(parentSingle.getRunBy().getFirstName());
    Assert.assertNotNull(parentSingle.getRunBy().getLastName());
  }

  @Test
  public void testInitJobExecutionsWith2Files() {
    // given
    int expectedParentJobExecutions = 1;
    int expectedChildJobExecutions = 2;
    int expectedJobExecutionsNumber = expectedParentJobExecutions + expectedChildJobExecutions;

    // when
    InitJobExecutionsRsDto response =
      constructAndPostInitJobExecutionRqDto(expectedChildJobExecutions);

    // then
    String actualParentJobExecutionId = response.getParentJobExecutionId();
    List<JobExecution> actualJobExecutions = response.getJobExecutions();

    Assert.assertNotNull(actualParentJobExecutionId);
    assertEquals(expectedJobExecutionsNumber, actualJobExecutions.size());

    int actualParentJobExecutions = 0;
    int actualChildJobExecutions = 0;

    for (JobExecution actualJobExecution : actualJobExecutions) {
      if (JobExecution.SubordinationType.PARENT_MULTIPLE.equals(actualJobExecution.getSubordinationType())) {
        assertParent(actualJobExecution);
        actualParentJobExecutions++;
      } else {
        assertChild(actualJobExecution, actualParentJobExecutionId);
        actualChildJobExecutions++;
      }
    }

    assertEquals(expectedParentJobExecutions, actualParentJobExecutions);
    assertEquals(expectedChildJobExecutions, actualChildJobExecutions);
  }

  @Test
  public void testInitJobExecutionsWithNoFiles() {
    // given
    InitJobExecutionsRqDto requestDto = new InitJobExecutionsRqDto();
    requestDto.setUserId(UUID.randomUUID().toString());
    requestDto.setSourceType(InitJobExecutionsRqDto.SourceType.FILES);

    // when
    RestAssured.given()
      .spec(spec)
      .body(JsonObject.mapFrom(requestDto).toString())
      .when().post(JOB_EXECUTION_PATH)
      .then().statusCode(HttpStatus.SC_BAD_REQUEST);
  }

  @Test
  public void testInitJobExecutionsWithUserWithoutPersonalInformation() {
    // given
    String jsonFiles;
    List<File> filesList;
    try {
      jsonFiles = TestUtil.readFileFromPath(FILES_PATH);
      filesList = new ObjectMapper().readValue(jsonFiles, new TypeReference<>() {
      });
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    List<File> limitedFilesList = filesList.stream().limit(1).collect(Collectors.toList());

    String stubUserId = UUID.randomUUID().toString();
    WireMock.stubFor(get(GET_USER_URL + stubUserId).willReturn(okJson(userResponse.toString())));
    InitJobExecutionsRqDto requestDto = new InitJobExecutionsRqDto();
    requestDto.getFiles().addAll(limitedFilesList);
    requestDto.setUserId(stubUserId);
    requestDto.setSourceType(InitJobExecutionsRqDto.SourceType.FILES);

    // when
    String parentJobExecutionId = RestAssured.given()
      .spec(spec)
      .body(JsonObject.mapFrom(requestDto).toString())
      .when().post(JOB_EXECUTION_PATH)
      .then().statusCode(HttpStatus.SC_CREATED)
      .extract().path("parentJobExecutionId");

    JsonObject jsonUser = userResponse.getJsonArray("users").getJsonObject(0);

    RestAssured.given()
      .spec(spec)
      .when()
      .get(JOB_EXECUTION_PATH + parentJobExecutionId)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("id", is(parentJobExecutionId))
      .body("hrId", greaterThanOrEqualTo(0))
      .body("runBy.firstName", is(jsonUser.getString("username")))
      .body("runBy.lastName", is("SYSTEM"));
  }

  @Test
  public void shouldReturnBadRequestOnPutWhenNoJobExecutionPassedInBody() {
    RestAssured.given()
      .spec(spec)
      .body(new JsonObject().toString())
      .when()
      .put(JOB_EXECUTION_PATH + UUID.randomUUID().toString())
      .then()
      .statusCode(HttpStatus.SC_UNPROCESSABLE_ENTITY);
  }

  @Test
  public void shouldReturnNotFoundOnPutWhenRecordDoesNotExist() {
    RestAssured.given()
      .spec(spec)
      .body(JsonObject.mapFrom(jobExecution).toString())
      .when()
      .put(JOB_EXECUTION_PATH + jobExecution.getId())
      .then()
      .statusCode(HttpStatus.SC_NOT_FOUND);
  }

  @Test
  public void shouldUpdateSingleParentOnPut() {
    InitJobExecutionsRsDto response =
      constructAndPostInitJobExecutionRqDto(1);
    List<JobExecution> createdJobExecutions = response.getJobExecutions();
    assertThat(createdJobExecutions.size(), is(1));
    JobExecution singleParent = createdJobExecutions.get(0);
    assertThat(singleParent.getSubordinationType(), is(JobExecution.SubordinationType.PARENT_SINGLE));

    singleParent.setJobProfileInfo(
      new JobProfileInfo().withId(DEFAULT_INSTANCE_JOB_PROFILE_ID).withName("Marc jobs profile"));
    RestAssured.given()
      .spec(spec)
      .body(JsonObject.mapFrom(singleParent).toString())
      .when()
      .put(JOB_EXECUTION_PATH + singleParent.getId())
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("id", is(singleParent.getId()))
      .body("jobProfileInfo.name", is(singleParent.getJobProfileInfo().getName()));
  }

  @Test
  public void shouldReturnNotFoundOnGetByIdWhenRecordDoesNotExist() {
    RestAssured.given()
      .spec(spec)
      .when()
      .get(JOB_EXECUTION_PATH + jobExecution.getId())
      .then()
      .statusCode(HttpStatus.SC_NOT_FOUND);
  }

  @Test
  public void shouldReturnJobExecutionOnGetById() {
    InitJobExecutionsRsDto response =
      constructAndPostInitJobExecutionRqDto(2);
    List<JobExecution> createdJobExecutions = response.getJobExecutions();
    assertThat(createdJobExecutions.size(), is(3));

    RestAssured.given()
      .spec(spec)
      .when()
      .get(JOB_EXECUTION_PATH + createdJobExecutions.get(0).getId())
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("id", is(createdJobExecutions.get(0).getId()))
      .body("hrId", greaterThanOrEqualTo(0))
      .body("runBy.firstName", notNullValue())
      .body("runBy.lastName", notNullValue());
  }

  @Test
  public void shouldReturnNotFoundOnGetChildrenByIdWhenJobExecutionDoesNotExist() {
    RestAssured.given()
      .spec(spec)
      .when()
      .get(JOB_EXECUTION_PATH + UUID.randomUUID().toString() + CHILDREN_PATH)
      .then()
      .statusCode(HttpStatus.SC_NOT_FOUND);
  }

  @Test
  public void shouldReturnAllChildrenOfMultipleParentOnGetChildrenById() {
    InitJobExecutionsRsDto response =
      constructAndPostInitJobExecutionRqDto(25);
    List<JobExecution> createdJobExecutions = response.getJobExecutions();
    assertThat(createdJobExecutions.size(), is(26));
    JobExecution multipleParent = createdJobExecutions.stream()
      .filter(jobExec -> jobExec.getSubordinationType().equals(JobExecution.SubordinationType.PARENT_MULTIPLE))
      .findFirst().get();

    RestAssured.given()
      .spec(spec)
      .when()
      .get(JOB_EXECUTION_PATH + multipleParent.getId() + CHILDREN_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("jobExecutions.size()", is(createdJobExecutions.size() - 1))
      .body("totalRecords", is(createdJobExecutions.size() - 1))
      .body("jobExecutions*.subordinationType", everyItem(is(JobExecution.SubordinationType.CHILD.name())));
  }

  @Test
  public void shouldReturnLimitedCollectionOnGetChildrenById() {
    int limit = 15;
    InitJobExecutionsRsDto response =
      constructAndPostInitJobExecutionRqDto(25);
    List<JobExecution> createdJobExecutions = response.getJobExecutions();
    assertThat(createdJobExecutions.size(), is(26));
    JobExecution multipleParent = createdJobExecutions.stream()
      .filter(jobExec -> jobExec.getSubordinationType().equals(JobExecution.SubordinationType.PARENT_MULTIPLE))
      .findFirst().get();

    RestAssured.given()
      .spec(spec)
      .when()
      .get(JOB_EXECUTION_PATH + multipleParent.getId() + CHILDREN_PATH + "?limit=" + limit)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("jobExecutions.size()", is(limit))
      .body("totalRecords", is(createdJobExecutions.size() - 1))
      .body("jobExecutions*.subordinationType", everyItem(is(JobExecution.SubordinationType.CHILD.name())));
  }

  @Test
  public void shouldReturnEmptyCollectionOnGetChildrenByIdInCaseOfSingleParent() {
    InitJobExecutionsRsDto response =
      constructAndPostInitJobExecutionRqDto(1);
    List<JobExecution> createdJobExecutions = response.getJobExecutions();
    assertThat(createdJobExecutions.size(), is(1));
    JobExecution jobExec = createdJobExecutions.get(0);

    RestAssured.given()
      .spec(spec)
      .when()
      .get(JOB_EXECUTION_PATH + jobExec.getId() + CHILDREN_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("jobExecutions", empty())
      .body("totalRecords", is(0));
  }

  @Test
  public void shouldReturnEmptyCollectionOnGetChildrenByIdInCaseOfChild() {
    InitJobExecutionsRsDto response =
      constructAndPostInitJobExecutionRqDto(3);
    List<JobExecution> createdJobExecutions = response.getJobExecutions();
    assertThat(createdJobExecutions.size(), is(4));
    JobExecution child = createdJobExecutions.stream()
      .filter(jobExec -> jobExec.getSubordinationType().equals(JobExecution.SubordinationType.CHILD)).findFirst().get();

    RestAssured.given()
      .spec(spec)
      .when()
      .get(JOB_EXECUTION_PATH + child.getId() + CHILDREN_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("jobExecutions", empty())
      .body("totalRecords", is(0));
  }

  @Test
  public void shouldReturnNotFoundOnStatusUpdate() {
    StatusDto status = new StatusDto().withStatus(StatusDto.Status.NEW);
    RestAssured.given()
      .spec(spec)
      .body(JsonObject.mapFrom(status).toString())
      .when()
      .put(JOB_EXECUTION_PATH + UUID.randomUUID().toString() + STATUS_PATH)
      .then()
      .statusCode(HttpStatus.SC_NOT_FOUND);
  }

  @Test
  public void shouldReturnBadRequestOnStatusUpdateWhenNoEntityPassed() {
    RestAssured.given()
      .spec(spec)
      .body(new JsonObject().toString())
      .when()
      .put(JOB_EXECUTION_PATH + UUID.randomUUID().toString() + STATUS_PATH)
      .then()
      .statusCode(HttpStatus.SC_UNPROCESSABLE_ENTITY);
  }

  @Test
  public void shouldReturnBadRequestOnStatusUpdate() {
    JsonObject status = new JsonObject().put("status", "Nonsense");
    RestAssured.given()
      .spec(spec)
      .body(status.toString())
      .when()
      .put(JOB_EXECUTION_PATH + UUID.randomUUID().toString() + STATUS_PATH)
      .then()
      .statusCode(HttpStatus.SC_BAD_REQUEST);
  }

  @Test
  public void shouldUpdateStatusOfSingleParent() {
    InitJobExecutionsRsDto response =
      constructAndPostInitJobExecutionRqDto(1);
    List<JobExecution> createdJobExecutions = response.getJobExecutions();
    assertThat(createdJobExecutions.size(), is(1));
    JobExecution jobExec = createdJobExecutions.get(0);

    StatusDto status = new StatusDto().withStatus(StatusDto.Status.PARSING_IN_PROGRESS);
    RestAssured.given()
      .spec(spec)
      .body(JsonObject.mapFrom(status).toString())
      .when()
      .put(JOB_EXECUTION_PATH + jobExec.getId() + STATUS_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("status", is(status.getStatus().name()))
      .body("uiStatus", is(Status.valueOf(status.getStatus().name()).getUiStatus()));

    RestAssured.given()
      .spec(spec)
      .when()
      .get(JOB_EXECUTION_PATH + jobExec.getId())
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("status", is(status.getStatus().name()))
      .body("uiStatus", is(Status.valueOf(status.getStatus().name()).getUiStatus()));
  }

  @Test
  public void shouldUpdateStatusOfChild() {
    InitJobExecutionsRsDto response =
      constructAndPostInitJobExecutionRqDto(3);
    List<JobExecution> createdJobExecutions = response.getJobExecutions();
    assertThat(createdJobExecutions.size(), is(4));
    JobExecution child = createdJobExecutions.stream()
      .filter(jobExec -> jobExec.getSubordinationType().equals(JobExecution.SubordinationType.CHILD)).findFirst().get();

    StatusDto status = new StatusDto().withStatus(StatusDto.Status.PARSING_IN_PROGRESS);
    RestAssured.given()
      .spec(spec)
      .body(JsonObject.mapFrom(status).toString())
      .when()
      .put(JOB_EXECUTION_PATH + child.getId() + STATUS_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("status", is(status.getStatus().name()))
      .body("uiStatus", is(Status.valueOf(status.getStatus().name()).getUiStatus()));

    RestAssured.given()
      .spec(spec)
      .when()
      .get(JOB_EXECUTION_PATH + child.getId())
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("status", is(status.getStatus().name()))
      .body("uiStatus", is(Status.valueOf(status.getStatus().name()).getUiStatus()));
  }

  @Test
  public void shouldUpdateStatusOfCompositeParent() {
    List<JobExecution> executions = constructAndPostCompositeInitJobExecutionRqDto("test", 1);
    assertThat(executions.size(), is(2));
    JobExecution parent = executions.get(0);
    JobExecution child = executions.get(1);

    // update child to a non-completion state; should NOT update parent
    StatusDto progressStatus = new StatusDto().withStatus(StatusDto.Status.PARSING_IN_PROGRESS);
    RestAssured.given()
      .spec(spec)
      .body(JsonObject.mapFrom(progressStatus).toString())
      .when()
      .put(JOB_EXECUTION_PATH + child.getId() + STATUS_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK);

    RestAssured.given()
      .spec(spec)
      .when()
      .get(JOB_EXECUTION_PATH + parent.getId())
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("status", is(not(JobExecution.Status.COMMITTED)))
      .body("uiStatus", is(not(JobExecution.UiStatus.RUNNING_COMPLETE)));

    // update child to a completion state; should now update parent
    StatusDto completeStatus = new StatusDto().withStatus(StatusDto.Status.COMMITTED);
    RestAssured.given()
      .spec(spec)
      .body(JsonObject.mapFrom(completeStatus).toString())
      .when()
      .put(JOB_EXECUTION_PATH + child.getId() + STATUS_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK);

    RestAssured.given()
      .spec(spec)
      .when()
      .get(JOB_EXECUTION_PATH + parent.getId())
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("status", is(JobExecution.Status.COMMITTED.toString()))
      .body("uiStatus", is(JobExecution.UiStatus.RUNNING_COMPLETE.toString()));
  }

  @Test
  public void shouldUpdateStatusOfCompositeParentMultipleChildren() {
    List<JobExecution> executions = constructAndPostCompositeInitJobExecutionRqDto("test", 2);
    assertThat(executions.size(), is(3));
    JobExecution parent = executions.get(0);
    JobExecution child1 = executions.get(1);
    JobExecution child2 = executions.get(2);

    // complete child1; parent should NOT update yet though, since child 2 is not completed
    StatusDto completeStatus = new StatusDto().withStatus(StatusDto.Status.COMMITTED);
    RestAssured.given()
      .spec(spec)
      .body(JsonObject.mapFrom(completeStatus).toString())
      .when()
      .put(JOB_EXECUTION_PATH + child1.getId() + STATUS_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK);

    RestAssured.given()
      .spec(spec)
      .when()
      .get(JOB_EXECUTION_PATH + parent.getId())
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("status", is(not(JobExecution.Status.COMMITTED.toString())))
      .body("uiStatus", is(not(JobExecution.UiStatus.RUNNING_COMPLETE.toString())));

    // after this, both children are completed so the parent should complete
    RestAssured.given()
      .spec(spec)
      .body(JsonObject.mapFrom(completeStatus).toString())
      .when()
      .put(JOB_EXECUTION_PATH + child2.getId() + STATUS_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK);

    RestAssured.given()
      .spec(spec)
      .when()
      .get(JOB_EXECUTION_PATH + parent.getId())
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("status", is(JobExecution.Status.COMMITTED.toString()))
      .body("uiStatus", is(JobExecution.UiStatus.RUNNING_COMPLETE.toString()));
  }

  @Test
  public void shouldNotUpdateStatusOfCompositeUnknownParent() {
    InitJobExecutionsRqDto childRequestDto = new InitJobExecutionsRqDto();
    childRequestDto.getFiles().add(new File().withName("test"));
    childRequestDto.setUserId(okapiUserIdHeader);
    childRequestDto.setSourceType(InitJobExecutionsRqDto.SourceType.COMPOSITE);
    childRequestDto.setParentJobId("aaaaaaaa-bbbb-1ccc-8ddd-eeeeeeeeeeee");
    childRequestDto.setJobPartNumber(1);
    childRequestDto.setTotalJobParts(1);

    JobExecution child =
      RestAssured.given()
        .spec(spec)
        .body(JsonObject.mapFrom(childRequestDto).toString())
        .when().post(JOB_EXECUTION_PATH).body().as(InitJobExecutionsRsDto.class).getJobExecutions().get(0);

    // update child to a completion state; should now attempt to update parent
    // the parent does not exist, though, so it should throw a 500
    StatusDto completeStatus = new StatusDto().withStatus(StatusDto.Status.COMMITTED);
    RestAssured.given()
      .spec(spec)
      .body(JsonObject.mapFrom(completeStatus).toString())
      .when()
      .put(JOB_EXECUTION_PATH + child.getId() + STATUS_PATH)
      .then()
      .statusCode(HttpStatus.SC_INTERNAL_SERVER_ERROR);
  }

  @Test
  public void shouldSetCompletedDateToJobExecutionOnUpdateStatusToError() {
    InitJobExecutionsRsDto response =
      constructAndPostInitJobExecutionRqDto(1);
    List<JobExecution> createdJobExecutions = response.getJobExecutions();
    assertThat(createdJobExecutions.size(), is(1));
    JobExecution jobExecution = createdJobExecutions.get(0);
    assertThat(jobExecution.getSubordinationType(), is(JobExecution.SubordinationType.PARENT_SINGLE));
    Assert.assertNotNull(jobExecution.getRunBy().getFirstName());
    Assert.assertNotNull(jobExecution.getRunBy().getLastName());

    StatusDto status = new StatusDto().withStatus(ERROR).withErrorStatus(StatusDto.ErrorStatus.FILE_PROCESSING_ERROR);
    RestAssured.given()
      .spec(spec)
      .body(JsonObject.mapFrom(status).toString())
      .when()
      .put(JOB_EXECUTION_PATH + jobExecution.getId() + STATUS_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("status", is(status.getStatus().name()))
      .body("uiStatus", is(Status.valueOf(status.getStatus().name()).getUiStatus()))
      .body("errorStatus", is(status.getErrorStatus().toString()))
      .body("completedDate", notNullValue(Date.class));

    RestAssured.given()
      .spec(spec)
      .when()
      .get(JOB_EXECUTION_PATH + jobExecution.getId())
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("status", is(status.getStatus().name()))
      .body("uiStatus", is(Status.valueOf(status.getStatus().name()).getUiStatus()))
      .body("errorStatus", is(status.getErrorStatus().toString()))
      .body("completedDate", notNullValue(Date.class));
  }

  @Test
  public void shouldSetTotalZeroToJobExecutionOnUpdateStatusToError() {
    InitJobExecutionsRsDto response =
      constructAndPostInitJobExecutionRqDto(1);
    List<JobExecution> createdJobExecutions = response.getJobExecutions();
    assertThat(createdJobExecutions.size(), is(1));
    JobExecution jobExecution = createdJobExecutions.get(0);
    assertThat(jobExecution.getSubordinationType(), is(JobExecution.SubordinationType.PARENT_SINGLE));
    Assert.assertNotNull(jobExecution.getRunBy().getFirstName());
    Assert.assertNotNull(jobExecution.getRunBy().getLastName());

    StatusDto status = new StatusDto().withStatus(ERROR).withErrorStatus(StatusDto.ErrorStatus.FILE_PROCESSING_ERROR);
    RestAssured.given()
      .spec(spec)
      .body(JsonObject.mapFrom(status).toString())
      .when()
      .put(JOB_EXECUTION_PATH + jobExecution.getId() + STATUS_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("status", is(status.getStatus().name()))
      .body("uiStatus", is(Status.valueOf(status.getStatus().name()).getUiStatus()))
      .body("errorStatus", is(status.getErrorStatus().toString()))
      .body("completedDate", notNullValue(Date.class));

    RestAssured.given()
      .spec(spec)
      .when()
      .get(JOB_EXECUTION_PATH + jobExecution.getId())
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("status", is(status.getStatus().name()))
      .body("uiStatus", is(Status.valueOf(status.getStatus().name()).getUiStatus()))
      .body("errorStatus", is(status.getErrorStatus().toString()))
      .body("completedDate", notNullValue(Date.class))
      .body("progress.total", is(0));
  }

  @Test
  public void shouldNotUpdateStatusToParent() {
    InitJobExecutionsRsDto response =
      constructAndPostInitJobExecutionRqDto(1);
    List<JobExecution> createdJobExecutions = response.getJobExecutions();
    assertThat(createdJobExecutions.size(), is(1));
    JobExecution jobExec = createdJobExecutions.get(0);

    StatusDto status = new StatusDto().withStatus(StatusDto.Status.PARENT);
    RestAssured.given()
      .spec(spec)
      .body(JsonObject.mapFrom(status).toString())
      .when()
      .put(JOB_EXECUTION_PATH + jobExec.getId() + STATUS_PATH)
      .then()
      .statusCode(HttpStatus.SC_BAD_REQUEST);

    RestAssured.given()
      .spec(spec)
      .when()
      .get(JOB_EXECUTION_PATH + jobExec.getId())
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("status", is(jobExec.getStatus().name()))
      .body("uiStatus", is(jobExec.getUiStatus().name()));
  }

  @Test
  public void shouldNotUpdateStatusOfParentMultiple() {
    InitJobExecutionsRsDto response =
      constructAndPostInitJobExecutionRqDto(3);
    List<JobExecution> createdJobExecutions = response.getJobExecutions();
    assertThat(createdJobExecutions.size(), is(4));
    JobExecution parent = createdJobExecutions.stream()
      .filter(jobExec -> jobExec.getSubordinationType().equals(JobExecution.SubordinationType.PARENT_MULTIPLE))
      .findFirst().get();

    StatusDto status = new StatusDto().withStatus(StatusDto.Status.PARSING_IN_PROGRESS);
    RestAssured.given()
      .spec(spec)
      .body(JsonObject.mapFrom(status).toString())
      .when()
      .put(JOB_EXECUTION_PATH + parent.getId() + STATUS_PATH)
      .then()
      .statusCode(HttpStatus.SC_BAD_REQUEST);

    RestAssured.given()
      .spec(spec)
      .when()
      .get(JOB_EXECUTION_PATH + parent.getId())
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("status", is(JobExecution.Status.PARENT.name()))
      .body("uiStatus", is(JobExecution.UiStatus.PARENT.name()));
  }

  @Test
  public void shouldUpdateMultipleParentOnPut() {
    InitJobExecutionsRsDto response =
      constructAndPostInitJobExecutionRqDto(2);
    List<JobExecution> createdJobExecutions = response.getJobExecutions();
    assertThat(createdJobExecutions.size(), is(3));
    JobExecution multipleParent = createdJobExecutions.stream()
      .filter(jobExec -> jobExec.getSubordinationType().equals(JobExecution.SubordinationType.PARENT_MULTIPLE))
      .findFirst().get();

    multipleParent.setJobProfileInfo(
      new JobProfileInfo().withId(DEFAULT_INSTANCE_JOB_PROFILE_ID).withName("Marc jobs profile"));
    RestAssured.given()
      .spec(spec)
      .body(JsonObject.mapFrom(multipleParent).toString())
      .when()
      .put(JOB_EXECUTION_PATH + multipleParent.getId())
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("id", is(multipleParent.getId()))
      .body("jobProfileInfo.name", is(multipleParent.getJobProfileInfo().getName()));

    RestAssured.given()
      .spec(spec)
      .when()
      .get(JOB_EXECUTION_PATH + multipleParent.getId())
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("jobProfileInfo.name", is(multipleParent.getJobProfileInfo().getName()));
  }

  @Test
  public void shouldNotUpdateMultipleParentStatusOnPut() {
    InitJobExecutionsRsDto response =
      constructAndPostInitJobExecutionRqDto(2);
    List<JobExecution> createdJobExecutions = response.getJobExecutions();
    assertThat(createdJobExecutions.size(), is(3));
    JobExecution multipleParent = createdJobExecutions.stream()
      .filter(jobExec -> jobExec.getSubordinationType().equals(JobExecution.SubordinationType.PARENT_MULTIPLE))
      .findFirst().get();

    multipleParent.setStatus(JobExecution.Status.PARSING_IN_PROGRESS);
    RestAssured.given()
      .spec(spec)
      .body(JsonObject.mapFrom(multipleParent).toString())
      .when()
      .put(JOB_EXECUTION_PATH + multipleParent.getId())
      .then()
      .statusCode(HttpStatus.SC_BAD_REQUEST);

    RestAssured.given()
      .spec(spec)
      .when()
      .get(JOB_EXECUTION_PATH + multipleParent.getId())
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("status", is(JobExecution.Status.PARENT.name()))
      .body("uiStatus", is(JobExecution.UiStatus.PARENT.name()));
  }

  @Test
  public void shouldNotUpdateStatusToParentOnPut() {
    InitJobExecutionsRsDto response =
      constructAndPostInitJobExecutionRqDto(1);
    List<JobExecution> createdJobExecutions = response.getJobExecutions();
    assertThat(createdJobExecutions.size(), is(1));
    JobExecution jobExec = createdJobExecutions.get(0);

    jobExec.setStatus(JobExecution.Status.PARENT);
    RestAssured.given()
      .spec(spec)
      .body(JsonObject.mapFrom(jobExec).toString())
      .when()
      .put(JOB_EXECUTION_PATH + jobExec.getId())
      .then()
      .statusCode(HttpStatus.SC_BAD_REQUEST);

    RestAssured.given()
      .spec(spec)
      .when()
      .get(JOB_EXECUTION_PATH + jobExec.getId())
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("status", is(JobExecution.Status.NEW.name()))
      .body("uiStatus", is(JobExecution.UiStatus.INITIALIZATION.name()));
  }

  @Test
  public void shouldReturnNotFoundOnSetJobProfileInfo() {
    JobProfileInfo jobProfile = new JobProfileInfo().withId(DEFAULT_INSTANCE_JOB_PROFILE_ID).withName("marc");
    RestAssured.given()
      .spec(spec)
      .body(JsonObject.mapFrom(jobProfile).toString())
      .when()
      .put(JOB_EXECUTION_PATH + UUID.randomUUID().toString() + JOB_PROFILE_PATH)
      .then()
      .statusCode(HttpStatus.SC_NOT_FOUND);
  }

  @Test
  public void shouldReturnBadRequestOnSetJobProfileInfo() {
    JobProfileInfo jobProfile = new JobProfileInfo().withName("Nonsense");
    RestAssured.given()
      .spec(spec)
      .body(JsonObject.mapFrom(jobProfile).toString())
      .when()
      .put(JOB_EXECUTION_PATH + UUID.randomUUID().toString() + JOB_PROFILE_PATH)
      .then()
      .statusCode(HttpStatus.SC_UNPROCESSABLE_ENTITY);
  }

  @Test
  public void shouldReturnBadRequestOnSetJobProfileInfoWhenJobExecutionAssociatedWithJobProfile() {
    InitJobExecutionsRsDto response =
      constructAndPostInitJobExecutionRqDto(1);
    List<JobExecution> createdJobExecutions = response.getJobExecutions();
    assertThat(createdJobExecutions.size(), is(1));
    JobExecution jobExec = createdJobExecutions.get(0);

    JobProfileInfo jobProfile = new JobProfileInfo().withId(DEFAULT_INSTANCE_JOB_PROFILE_ID).withName("marc");
    RestAssured.given()
      .spec(spec)
      .body(JsonObject.mapFrom(jobProfile).toString())
      .when()
      .put(JOB_EXECUTION_PATH + jobExec.getId() + JOB_PROFILE_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK);

    RestAssured.given()
      .spec(spec)
      .body(JsonObject.mapFrom(jobProfile).toString())
      .when()
      .put(JOB_EXECUTION_PATH + jobExec.getId() + JOB_PROFILE_PATH)
      .then()
      .statusCode(HttpStatus.SC_BAD_REQUEST);
  }

  @Test
  public void shouldMarkJobExecutionAsErrorOnSetJobProfileInfoWhenCreationProfileWrapperFailed() {
    InitJobExecutionsRsDto response =
      constructAndPostInitJobExecutionRqDto(1);
    List<JobExecution> createdJobExecutions = response.getJobExecutions();
    assertThat(createdJobExecutions.size(), is(1));
    JobExecution jobExec = createdJobExecutions.get(0);

    WireMock.stubFor(post(new UrlPathPattern(new RegexPattern(PROFILE_SNAPSHOT_URL + "/.*"), true))
      .willReturn(serverError()));

    JobProfileInfo jobProfile = new JobProfileInfo().withId(DEFAULT_INSTANCE_JOB_PROFILE_ID).withName("marc");
    RestAssured.given()
      .spec(spec)
      .body(JsonObject.mapFrom(jobProfile).toString())
      .when()
      .put(JOB_EXECUTION_PATH + jobExec.getId() + JOB_PROFILE_PATH)
      .then()
      .statusCode(HttpStatus.SC_INTERNAL_SERVER_ERROR);

    RestAssured.given()
      .spec(spec)
      .when()
      .get(JOB_EXECUTION_PATH + jobExec.getId())
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("status", is(JobExecution.Status.ERROR.value()))
      .body("errorStatus", is(JobExecution.ErrorStatus.PROFILE_SNAPSHOT_CREATING_ERROR.value()));
  }

  @Test
  public void shouldSetJobProfileInfoForJobExecution() {
    InitJobExecutionsRsDto response =
      constructAndPostInitJobExecutionRqDto(1);
    List<JobExecution> createdJobExecutions = response.getJobExecutions();
    assertThat(createdJobExecutions.size(), is(1));
    JobExecution jobExec = createdJobExecutions.get(0);

    JobProfileInfo jobProfile =
      new JobProfileInfo().withId(DEFAULT_INSTANCE_JOB_PROFILE_ID).withName("Default job profile");
    RestAssured.given()
      .spec(spec)
      .body(JsonObject.mapFrom(jobProfile).toString())
      .when()
      .put(JOB_EXECUTION_PATH + jobExec.getId() + JOB_PROFILE_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("jobProfileInfo.id", is(jobProfile.getId()))
      .body("jobProfileInfo.name", is(jobProfile.getName()))
      .body("jobProfileSnapshotWrapper", notNullValue());

    RestAssured.given()
      .spec(spec)
      .when()
      .get(JOB_EXECUTION_PATH + jobExec.getId())
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("jobProfileInfo.id", is(jobProfile.getId()))
      .body("jobProfileInfo.name", is(jobProfile.getName()))
      .body("jobProfileSnapshotWrapper", notNullValue());
  }

  @Test
  public void shouldReturnBadRequestOnPostWhenNoDtoPassedInBody() {
    RestAssured.given()
      .spec(spec)
      .body(new JsonObject().toString())
      .when()
      .post(JOB_EXECUTION_PATH + UUID.randomUUID().toString() + RECORDS_PATH)
      .then()
      .statusCode(HttpStatus.SC_UNPROCESSABLE_ENTITY);
  }

  @Test
  public void shouldReturnErrorOnPostRawRecordsWhenJobExecutionDoesNotExist() {
    RestAssured.given()
      .spec(spec)
      .body(rawRecordsDto)
      .when()
      .post(JOB_EXECUTION_PATH + UUID.randomUUID().toString() + RECORDS_PATH)
      .then()
      .statusCode(HttpStatus.SC_NOT_FOUND);
  }

  @Test
  public void shouldProcessChunkOfRawRecords(TestContext testContext) {
    InitJobExecutionsRsDto response =
      constructAndPostInitJobExecutionRqDto(1);
    List<JobExecution> createdJobExecutions = response.getJobExecutions();
    assertThat(createdJobExecutions.size(), is(1));
    JobExecution jobExec = createdJobExecutions.get(0);

    WireMock.stubFor(post(RECORDS_SERVICE_URL)
      .willReturn(created().withTransformers(RequestToResponseTransformer.NAME)));

    Async async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .body(new JobProfileInfo()
        .withName("MARC records")
        .withId(DEFAULT_INSTANCE_JOB_PROFILE_ID)
        .withDataType(JobProfileInfo.DataType.MARC))
      .when()
      .put(JOB_EXECUTION_PATH + jobExec.getId() + JOB_PROFILE_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK);
    async.complete();

    async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .body(rawRecordsDto)
      .when()
      .post(JOB_EXECUTION_PATH + jobExec.getId() + RECORDS_PATH)
      .then()
      .statusCode(HttpStatus.SC_NO_CONTENT);
    async.complete();

    async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .when()
      .get(JOB_EXECUTION_PATH + jobExec.getId())
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("status", is(JobExecution.Status.PARSING_IN_PROGRESS.name()))
      .body("runBy.firstName", is("DIKU"))
      .body("progress.total", is(rawRecordsDto.getRecordsMetadata().getTotal()))
      .body("startedDate", notNullValue(Date.class)).log().all();
    async.complete();
  }

  @Test
  public void shouldProcessChunkOfRawRecordsWith999SubfieldByAcceptInstanceIdParameter(TestContext testContext) {
    InitJobExecutionsRsDto response =
      constructAndPostInitJobExecutionRqDto(1);
    List<JobExecution> createdJobExecutions = response.getJobExecutions();
    assertThat(createdJobExecutions.size(), is(1));
    JobExecution jobExec = createdJobExecutions.get(0);

    WireMock.stubFor(post(RECORDS_SERVICE_URL)
      .willReturn(created().withTransformers(RequestToResponseTransformer.NAME)));

    Async async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .body(new JobProfileInfo()
        .withName("MARC records")
        .withId(DEFAULT_INSTANCE_JOB_PROFILE_ID)
        .withDataType(JobProfileInfo.DataType.MARC))
      .when()
      .put(JOB_EXECUTION_PATH + jobExec.getId() + JOB_PROFILE_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK);
    async.complete();

    async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .body(rawRecordsDto_3)
      .when()
      .post(JOB_EXECUTION_PATH + jobExec.getId() + RECORDS_PATH + "?acceptInstanceId=true")
      .then()
      .statusCode(HttpStatus.SC_NO_CONTENT);
    async.complete();
  }

  @Test
  public void shouldProcessChunkOfRawRecordsWithDuplicatedTags(TestContext testContext) {
    InitJobExecutionsRsDto response =
      constructAndPostInitJobExecutionRqDto(1);
    List<JobExecution> createdJobExecutions = response.getJobExecutions();
    assertThat(createdJobExecutions.size(), is(1));
    JobExecution jobExec = createdJobExecutions.get(0);

    WireMock.stubFor(post(RECORDS_SERVICE_URL)
      .willReturn(created().withTransformers(RequestToResponseTransformer.NAME)));

    Async async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .body(new JobProfileInfo()
        .withName("MARC records")
        .withId(DEFAULT_INSTANCE_JOB_PROFILE_ID)
        .withDataType(JobProfileInfo.DataType.MARC))
      .when()
      .put(JOB_EXECUTION_PATH + jobExec.getId() + JOB_PROFILE_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK);
    async.complete();

    async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .body(rawRecordsDto_2.withId(UUID.randomUUID().toString()))
      .when()
      .post(JOB_EXECUTION_PATH + jobExec.getId() + RECORDS_PATH)
      .then()
      .statusCode(HttpStatus.SC_NO_CONTENT);
    async.complete();

    async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .when()
      .get(JOB_EXECUTION_PATH + jobExec.getId())
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("runBy.firstName", is("DIKU"))
      .body("progress.total", is(rawRecordsDto_2.getRecordsMetadata().getTotal()))
      .body("startedDate", notNullValue(Date.class)).log().all();
    async.complete();
  }

  @Test
  public void shouldProcessChunksAndRequestForMappingParameters1Time(TestContext testContext) {
    // given
    InitJobExecutionsRsDto response = constructAndPostInitJobExecutionRqDto(1);
    List<JobExecution> createdJobExecutions = response.getJobExecutions();
    assertThat(createdJobExecutions.size(), is(1));
    JobExecution jobExec = createdJobExecutions.get(0);
    WireMock.stubFor(
      post(RECORDS_SERVICE_URL).willReturn(created().withTransformers(RequestToResponseTransformer.NAME)));

    Async async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .body(new JobProfileInfo()
        .withName("MARC records")
        .withId(DEFAULT_INSTANCE_JOB_PROFILE_ID)
        .withDataType(JobProfileInfo.DataType.MARC))
      .when()
      .put(JOB_EXECUTION_PATH + jobExec.getId() + JOB_PROFILE_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK);
    async.complete();

    // when
    async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .body(rawRecordsDto)
      .when()
      .post(JOB_EXECUTION_PATH + jobExec.getId() + RECORDS_PATH)
      .then()
      .statusCode(HttpStatus.SC_NO_CONTENT);
    async.complete();

    async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .body(rawRecordsDto)
      .when()
      .post(JOB_EXECUTION_PATH + jobExec.getId() + RECORDS_PATH)
      .then()
      .statusCode(HttpStatus.SC_INTERNAL_SERVER_ERROR);
    async.complete();

    // then
    async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .when()
      .get(JOB_EXECUTION_PATH + jobExec.getId())
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("status", is(JobExecution.Status.PARSING_IN_PROGRESS.name()))
      .body("runBy.firstName", is("DIKU"))
      .body("progress.total", is(rawRecordsDto.getRecordsMetadata().getTotal()))
      .body("startedDate", notNullValue(Date.class)).log().all();

    verify(1, getRequestedFor(urlEqualTo(IDENTIFIER_TYPES_URL)));
    verify(1, getRequestedFor(urlEqualTo(INSTANCE_TYPES_URL)));
    verify(1, getRequestedFor(urlEqualTo(CLASSIFICATION_TYPES_URL)));
    verify(1, getRequestedFor(urlEqualTo(INSTANCE_FORMATS_URL)));
    verify(1, getRequestedFor(urlEqualTo(CONTRIBUTOR_TYPES_URL)));
    verify(1, getRequestedFor(urlEqualTo(CONTRIBUTOR_NAME_TYPES_URL)));
    verify(1, getRequestedFor(urlEqualTo(ELECTRONIC_ACCESS_URL)));
    verify(1, getRequestedFor(urlEqualTo(INSTANCE_NOTE_TYPES_URL)));
    verify(1, getRequestedFor(urlEqualTo(INSTANCE_ALTERNATIVE_TITLE_TYPES_URL)));
    verify(1, getRequestedFor(urlEqualTo(MODE_OF_ISSUANCE_TYPES_URL)));
    verify(1, getRequestedFor(urlEqualTo(INSTANCE_STATUSES_URL)));
    verify(1, getRequestedFor(urlEqualTo(NATURE_OF_CONTENT_TERMS_URL)));
    verify(1, getRequestedFor(urlEqualTo(INSTANCE_RELATIONSHIP_TYPES_URL)));
    verify(1, getRequestedFor(urlEqualTo(HOLDINGS_TYPES_URL)));
    verify(1, getRequestedFor(urlEqualTo(HOLDINGS_NOTE_TYPES_URL)));
    verify(1, getRequestedFor(urlEqualTo(ILL_POLICIES_URL)));
    verify(1, getRequestedFor(urlEqualTo(CALL_NUMBER_TYPES_URL)));
    verify(1, getRequestedFor(urlEqualTo(STATISTICAL_CODES_URL)));
    verify(1, getRequestedFor(urlEqualTo(STATISTICAL_CODE_TYPES_URL)));
    verify(1, getRequestedFor(urlEqualTo(LOCATIONS_URL)));
    verify(1, getRequestedFor(urlEqualTo(MATERIAL_TYPES_URL)));
    verify(1, getRequestedFor(urlEqualTo(ITEM_DAMAGED_STATUSES_URL)));
    verify(1, getRequestedFor(urlEqualTo(LOAN_TYPES_URL)));
    verify(1, getRequestedFor(urlEqualTo(ITEM_NOTE_TYPES_URL)));
    verify(1, getRequestedFor(urlEqualTo(AUTHORITY_NOTE_TYPES_URL)));
    verify(1, getRequestedFor(urlEqualTo(AUTHORITY_SOURCE_FILES_URL)));
    verify(1, getRequestedFor(urlEqualTo(SUBJECT_SOURCES_URL)));
    verify(1, getRequestedFor(urlEqualTo(SUBJECT_TYPES_URL)));
    verify(1, getRequestedFor(urlEqualTo(FIELD_PROTECTION_SETTINGS_URL)));
    verify(1, getRequestedFor(urlEqualTo(TENANT_CONFIGURATIONS_SETTINGS_URL)));
    async.complete();
  }

  @Test
  public void shouldProcessChunkIfRequestForMappingParametersFails(TestContext testContext) {
    // given
    InitJobExecutionsRsDto response = constructAndPostInitJobExecutionRqDto(1);
    List<JobExecution> createdJobExecutions = response.getJobExecutions();
    assertThat(createdJobExecutions.size(), is(1));
    JobExecution jobExec = createdJobExecutions.get(0);
    WireMock.stubFor(
      post(RECORDS_SERVICE_URL).willReturn(created().withTransformers(RequestToResponseTransformer.NAME)));

    // Do mock services to return failed response
    WireMock.stubFor(get(IDENTIFIER_TYPES_URL).willReturn(serverError()));
    WireMock.stubFor(get(INSTANCE_TYPES_URL).willReturn(serverError()));
    WireMock.stubFor(get(CLASSIFICATION_TYPES_URL).willReturn(serverError()));
    WireMock.stubFor(get(ELECTRONIC_ACCESS_URL).willReturn(serverError()));
    WireMock.stubFor(get(INSTANCE_FORMATS_URL).willReturn(serverError()));
    WireMock.stubFor(get(CONTRIBUTOR_NAME_TYPES_URL).willReturn(serverError()));
    WireMock.stubFor(get(CONTRIBUTOR_TYPES_URL).willReturn(serverError()));
    WireMock.stubFor(get(INSTANCE_NOTE_TYPES_URL).willReturn(serverError()));
    WireMock.stubFor(get(INSTANCE_ALTERNATIVE_TITLE_TYPES_URL).willReturn(serverError()));
    WireMock.stubFor(get(MODE_OF_ISSUANCE_TYPES_URL).willReturn(serverError()));
    WireMock.stubFor(get(INSTANCE_STATUSES_URL).willReturn(serverError()));
    WireMock.stubFor(get(NATURE_OF_CONTENT_TERMS_URL).willReturn(serverError()));
    WireMock.stubFor(get(INSTANCE_RELATIONSHIP_TYPES_URL).willReturn(serverError()));
    WireMock.stubFor(get(HOLDINGS_TYPES_URL).willReturn(serverError()));
    WireMock.stubFor(get(HOLDINGS_NOTE_TYPES_URL).willReturn(serverError()));
    WireMock.stubFor(get(ILL_POLICIES_URL).willReturn(serverError()));
    WireMock.stubFor(get(CALL_NUMBER_TYPES_URL).willReturn(serverError()));
    WireMock.stubFor(get(STATISTICAL_CODES_URL).willReturn(serverError()));
    WireMock.stubFor(get(STATISTICAL_CODE_TYPES_URL).willReturn(serverError()));
    WireMock.stubFor(get(LOCATIONS_URL).willReturn(serverError()));
    WireMock.stubFor(get(MATERIAL_TYPES_URL).willReturn(serverError()));
    WireMock.stubFor(get(ITEM_DAMAGED_STATUSES_URL).willReturn(serverError()));
    WireMock.stubFor(get(LOAN_TYPES_URL).willReturn(serverError()));
    WireMock.stubFor(get(ITEM_NOTE_TYPES_URL).willReturn(serverError()));
    WireMock.stubFor(get(AUTHORITY_NOTE_TYPES_URL).willReturn(serverError()));
    WireMock.stubFor(get(AUTHORITY_SOURCE_FILES_URL).willReturn(serverError()));
    WireMock.stubFor(get(FIELD_PROTECTION_SETTINGS_URL).willReturn(serverError()));
    WireMock.stubFor(get(TENANT_CONFIGURATIONS_SETTINGS_URL).willReturn(serverError()));

    Async async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .body(new JobProfileInfo()
        .withName("MARC records")
        .withId(DEFAULT_INSTANCE_JOB_PROFILE_ID)
        .withDataType(JobProfileInfo.DataType.MARC))
      .when()
      .put(JOB_EXECUTION_PATH + jobExec.getId() + JOB_PROFILE_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK);
    async.complete();

    // when
    async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .body(rawRecordsDto.withId(UUID.randomUUID().toString()))
      .when()
      .post(JOB_EXECUTION_PATH + jobExec.getId() + RECORDS_PATH)
      .then()
      .statusCode(HttpStatus.SC_NO_CONTENT);
    async.complete();

    // then
    async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .when()
      .get(JOB_EXECUTION_PATH + jobExec.getId())
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("status", is(JobExecution.Status.PARSING_IN_PROGRESS.name()))
      .body("runBy.firstName", is("DIKU"))
      .body("progress.total", is(rawRecordsDto.getRecordsMetadata().getTotal()))
      .body("startedDate", notNullValue(Date.class)).log().all();

    async.complete();
  }

  @Test
  public void shouldProcessChunkOfRawRecordsIfAddingAdditionalFieldsFailed(TestContext testContext) {
    InitJobExecutionsRsDto response =
      constructAndPostInitJobExecutionRqDto(1);
    List<JobExecution> createdJobExecutions = response.getJobExecutions();
    assertThat(createdJobExecutions.size(), is(1));
    JobExecution jobExec = createdJobExecutions.get(0);

    WireMock.stubFor(WireMock.put(PARSED_RECORDS_COLLECTION_URL)
      .willReturn(WireMock.serverError()));

    WireMock.stubFor(post(RECORDS_SERVICE_URL)
      .willReturn(created().withTransformers(RequestToResponseTransformer.NAME)));

    Async async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .body(new JobProfileInfo()
        .withName("MARC records")
        .withId(DEFAULT_INSTANCE_JOB_PROFILE_ID)
        .withDataType(JobProfileInfo.DataType.MARC))
      .when()
      .put(JOB_EXECUTION_PATH + jobExec.getId() + JOB_PROFILE_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK);
    async.complete();

    async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .body(rawRecordsDto.withId(UUID.randomUUID().toString()))
      .when()
      .post(JOB_EXECUTION_PATH + jobExec.getId() + RECORDS_PATH)
      .then()
      .statusCode(HttpStatus.SC_NO_CONTENT);
    async.complete();

    async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .when()
      .get(JOB_EXECUTION_PATH + jobExec.getId())
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("status", is(JobExecution.Status.PARSING_IN_PROGRESS.name()))
      .body("runBy.firstName", is("DIKU"))
      .body("progress.total", is(rawRecordsDto.getRecordsMetadata().getTotal()))
      .body("startedDate", notNullValue(Date.class)).log().all();
    async.complete();
  }

  @Test
  public void shouldNotParseChunkOfRawRecordsIfRecordListEmpty() {
    InitJobExecutionsRsDto response =
      constructAndPostInitJobExecutionRqDto(1);
    List<JobExecution> createdJobExecutions = response.getJobExecutions();
    assertThat(createdJobExecutions.size(), is(1));
    JobExecution jobExec = createdJobExecutions.get(0);

    WireMock.stubFor(post(RECORDS_SERVICE_URL)
      .willReturn(created().withTransformers(RequestToResponseTransformer.NAME)));

    RestAssured.given()
      .spec(spec)
      .body(new JobProfileInfo()
        .withName("MARC records")
        .withId(DEFAULT_INSTANCE_JOB_PROFILE_ID)
        .withDataType(JobProfileInfo.DataType.MARC))
      .when()
      .put(JOB_EXECUTION_PATH + jobExec.getId() + JOB_PROFILE_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK);

    RestAssured.given()
      .spec(spec)
      .body(new RawRecordsDto()
        .withRecordsMetadata(new RecordsMetadata()
          .withContentType(RecordsMetadata.ContentType.MARC_RAW)
          .withLast(false)
          .withCounter(1)).withId(UUID.randomUUID().toString()))
      .when()
      .post(JOB_EXECUTION_PATH + jobExec.getId() + RECORDS_PATH)
      .then()
      .statusCode(HttpStatus.SC_NO_CONTENT);
  }

  @Test
  public void shouldProcessLastChunkOfRawRecords(TestContext testContext) {
    InitJobExecutionsRsDto response =
      constructAndPostInitJobExecutionRqDto(1);
    List<JobExecution> createdJobExecutions = response.getJobExecutions();
    assertThat(createdJobExecutions.size(), is(1));
    JobExecution jobExec = createdJobExecutions.get(0);
    jobExec.setRunBy(new RunBy().withFirstName("DIKU").withLastName("ADMINISTRATOR"));
    jobExec.setProgress(new Progress().withCurrent(1000).withTotal(1000));
    jobExec.setStartedDate(new Date());

    WireMock.stubFor(post(RECORDS_SERVICE_URL)
      .willReturn(created().withTransformers(RequestToResponseTransformer.NAME)));

    Async async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .body(jobExec)
      .when()
      .put(JOB_EXECUTION_PATH + jobExec.getId())
      .then()
      .statusCode(HttpStatus.SC_OK).log().all();
    async.complete();

    async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .body(new JobProfileInfo()
        .withName("MARC records")
        .withId(DEFAULT_INSTANCE_JOB_PROFILE_ID)
        .withDataType(JobProfileInfo.DataType.MARC))
      .when()
      .put(JOB_EXECUTION_PATH + jobExec.getId() + JOB_PROFILE_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK);
    async.complete();

    rawRecordsDto.getRecordsMetadata().setLast(true);
    async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .body(rawRecordsDto.withId(UUID.randomUUID().toString()))
      .when()
      .post(JOB_EXECUTION_PATH + jobExec.getId() + RECORDS_PATH)
      .then()
      .statusCode(HttpStatus.SC_NO_CONTENT);
    async.complete();

    async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .when()
      .get(JOB_EXECUTION_PATH + jobExec.getId())
      .then()
      .statusCode(HttpStatus.SC_OK);
    async.complete();
  }

  private void assertParent(JobExecution parent) {
    Assert.assertNotNull(parent);
    Assert.assertNotNull(parent.getId());
    Assert.assertNotNull(parent.getParentJobId());
    Assert.assertTrue(parentTypes.contains(parent.getSubordinationType()));
    assertEquals(parent.getId(), parent.getParentJobId());
    if (JobExecution.SubordinationType.PARENT_SINGLE.equals(parent.getSubordinationType())) {
      assertEquals(JobExecution.Status.NEW, parent.getStatus());
    } else {
      assertEquals(JobExecution.Status.PARENT, parent.getStatus());
    }
    if (JobExecution.SubordinationType.PARENT_SINGLE.equals(parent.getSubordinationType())) {
      //TODO assert source path properly
      Assert.assertNotNull(parent.getSourcePath());
      Assert.assertNotNull(parent.getFileName());
    }
  }

  private void assertChild(JobExecution child, String parentJobExecutionId) {
    Assert.assertNotNull(child);
    Assert.assertNotNull(child.getId());
    Assert.assertNotNull(child.getParentJobId());
    assertEquals(child.getParentJobId(), parentJobExecutionId);
    assertEquals(JobExecution.Status.NEW, child.getStatus());
    assertEquals(JobExecution.SubordinationType.CHILD, child.getSubordinationType());
    //TODO assert source path properly
    Assert.assertNotNull(child.getSourcePath());
    Assert.assertNotNull(child.getFileName());
  }

  @Test
  public void shouldUpdateSingleParentOnPutWhenSnapshotUpdateFailed() {
    WireMock.stubFor(WireMock.put(new UrlPathPattern(new RegexPattern(SNAPSHOT_SERVICE_URL + "/.*"), true))
      .willReturn(WireMock.serverError()));

    InitJobExecutionsRsDto response =
      constructAndPostInitJobExecutionRqDto(1);
    List<JobExecution> createdJobExecutions = response.getJobExecutions();
    assertThat(createdJobExecutions.size(), is(1));
    JobExecution singleParent = createdJobExecutions.get(0);
    assertThat(singleParent.getSubordinationType(), is(JobExecution.SubordinationType.PARENT_SINGLE));

    singleParent.setJobProfileInfo(
      new JobProfileInfo().withId(DEFAULT_INSTANCE_JOB_PROFILE_ID).withName("Marc jobs profile"));
    RestAssured.given()
      .spec(spec)
      .body(JsonObject.mapFrom(singleParent).toString())
      .when()
      .put(JOB_EXECUTION_PATH + singleParent.getId())
      .then()
      .statusCode(HttpStatus.SC_INTERNAL_SERVER_ERROR);
  }

  @Test
  public void shouldReturnErrorOnPostJobExecutionWhenFailedPostSnapshotToStorage() throws IOException {
    WireMock.stubFor(post(SNAPSHOT_SERVICE_URL)
      .willReturn(WireMock.serverError()));

    InitJobExecutionsRqDto requestDto = new InitJobExecutionsRqDto();
    String jsonFiles = TestUtil.readFileFromPath(FILES_PATH);
    List<File> filesList = new ObjectMapper().readValue(jsonFiles, new TypeReference<>() { });

    requestDto.getFiles().addAll(filesList.stream().limit(1).collect(Collectors.toList()));
    requestDto.setUserId(okapiUserIdHeader);
    requestDto.setSourceType(InitJobExecutionsRqDto.SourceType.FILES);
    RestAssured.given()
      .spec(spec)
      .body(JsonObject.mapFrom(requestDto).toString())
      .when()
      .post(JOB_EXECUTION_PATH)
      .then()
      .statusCode(HttpStatus.SC_INTERNAL_SERVER_ERROR);
  }

  @Test
  public void shouldNotPostRecordsToRecordsStorageWhenJobProfileSnapshotContainsUpdateMarcActionProfile(
    TestContext testContext) {
    InitJobExecutionsRsDto response =
      constructAndPostInitJobExecutionRqDto(1);
    List<JobExecution> createdJobExecutions = response.getJobExecutions();
    assertThat(createdJobExecutions.size(), is(1));
    JobExecution jobExec = createdJobExecutions.get(0);

    MatchProfile matchProfile = new MatchProfile()
      .withName("match 999ff $s to 999ff $s");
    ActionProfile updateMarcAction = new ActionProfile()
      .withName("update marc-bib")
      .withAction(ActionProfile.Action.UPDATE)
      .withFolioRecord(ActionProfile.FolioRecord.MARC_BIBLIOGRAPHIC);

    ProfileSnapshotWrapper profileSnapshotWithUpdateMarcAction = new ProfileSnapshotWrapper()
      .withId(UUID.randomUUID().toString())
      .withProfileId(jobProfile.getId())
      .withContentType(JOB_PROFILE)
      .withContent(jobProfile)
      .withChildSnapshotWrappers(singletonList(
        new ProfileSnapshotWrapper()
          .withContentType(MATCH_PROFILE)
          .withContent(JsonObject.mapFrom(matchProfile).getMap())
          .withChildSnapshotWrappers(singletonList(
            new ProfileSnapshotWrapper()
              .withContentType(ACTION_PROFILE)
              .withContent(JsonObject.mapFrom(updateMarcAction).getMap())))));

    WireMock.stubFor(post(RECORDS_SERVICE_URL)
      .willReturn(created().withTransformers(RequestToResponseTransformer.NAME)));
    WireMock.stubFor(post(new UrlPathPattern(new RegexPattern(PROFILE_SNAPSHOT_URL + "/.*"), true))
      .willReturn(WireMock.created().withBody(Json.encode(profileSnapshotWithUpdateMarcAction))));
    WireMock.stubFor(get(new UrlPathPattern(new RegexPattern(PROFILE_SNAPSHOT_URL + "/.*"), true))
      .willReturn(WireMock.ok().withBody(Json.encode(profileSnapshotWithUpdateMarcAction))));

    Async async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .body(new JobProfileInfo()
        .withName("MARC records")
        .withId(DEFAULT_INSTANCE_JOB_PROFILE_ID)
        .withDataType(JobProfileInfo.DataType.MARC))
      .when()
      .put(JOB_EXECUTION_PATH + jobExec.getId() + JOB_PROFILE_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK);
    async.complete();

    async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .body(rawRecordsDto.withId(UUID.randomUUID().toString()))
      .when()
      .post(JOB_EXECUTION_PATH + jobExec.getId() + RECORDS_PATH)
      .then()
      .statusCode(HttpStatus.SC_NO_CONTENT);
    async.complete();

    async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .when()
      .get(JOB_EXECUTION_PATH + jobExec.getId())
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("status", is(JobExecution.Status.PARSING_IN_PROGRESS.name()))
      .body("startedDate", notNullValue(Date.class));
    async.complete();

    verify(0, getRequestedFor(urlEqualTo(RECORDS_SERVICE_URL)));
  }

  @Test
  public void shouldFillInRecordOrderIfAtLeastOneMarcBibRecordHasNoOrder(TestContext testContext) throws InterruptedException {
    fillInRecordOrderIfAtLeastOneRecordHasNoOrder(testContext, CORRECT_RAW_RECORD_3);
  }

  @Test
  public void shouldFillInRecordOrderIfAtLeastOneMarcAuthorityRecordHasNoOrder(TestContext testContext) throws InterruptedException {
    fillInRecordOrderIfAtLeastOneRecordHasNoOrder(testContext, CORRECT_RAW_RECORD_3);
  }

  @Test
  public void shouldFillRecordOrderIfAtLeastOneMarcAuthorityRecordHasNoOrder(TestContext testContext) throws InterruptedException {
    fillInRecordOrderIfAtLeastOneRecordHasNoOrder(testContext, CORRECT_MARC_HOLDINGS_RAW_RECORD);
  }

  private void fillInRecordOrderIfAtLeastOneRecordHasNoOrder(TestContext testContext, String rawRecord) throws InterruptedException {
    RawRecordsDto rawRecordsDto = new RawRecordsDto()
      .withId(UUID.randomUUID().toString())
      .withRecordsMetadata(new RecordsMetadata()
        .withLast(true)
        .withCounter(7)
        .withContentType(RecordsMetadata.ContentType.MARC_RAW))
      .withInitialRecords(asList(
        new InitialRecord().withRecord(CORRECT_RAW_RECORD_1),
        new InitialRecord().withRecord(CORRECT_RAW_RECORD_2).withOrder(5),
        new InitialRecord().withRecord(rawRecord).withOrder(6)));

    InitJobExecutionsRsDto response = constructAndPostInitJobExecutionRqDto(1);
    List<JobExecution> createdJobExecutions = response.getJobExecutions();
    assertThat(createdJobExecutions.size(), is(1));
    JobExecution jobExec = createdJobExecutions.get(0);

    Async async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .body(new JobProfileInfo()
        .withName("MARC records")
        .withId(DEFAULT_INSTANCE_JOB_PROFILE_ID)
        .withDataType(DataType.MARC))
      .when()
      .put(JOB_EXECUTION_PATH + jobExec.getId() + JOB_PROFILE_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK);
    async.complete();

    async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .body(rawRecordsDto)
      .when()
      .post(JOB_EXECUTION_PATH + jobExec.getId() + RECORDS_PATH)
      .then()
      .statusCode(HttpStatus.SC_NO_CONTENT);
    async.complete();

    String topicToObserve = formatToKafkaTopicName(DI_INCOMING_MARC_BIB_RECORD_PARSED.value());
    final AtomicReference<DataImportEventPayload> eventPayloadRef = new AtomicReference<>();

    await().forever().pollInterval(1, TimeUnit.SECONDS).until(() -> {
      List<String> observedValues = kafkaCluster.observeValues(ObserveKeyValues.on(topicToObserve, 3)
        .build());
      return observedValues.stream()
        .anyMatch(value -> {
          Event event = Json.decodeValue(value, Event.class);
          if (event != null && event.getEventPayload() != null) {
            DataImportEventPayload payload = Json.decodeValue(event.getEventPayload(), DataImportEventPayload.class);
            if (payload != null && payload.getContext() != null && payload.getContext().containsKey("MARC_BIBLIOGRAPHIC")) {
              eventPayloadRef.set(payload);
              return true;
            }
          }
          return false;
        });
    });

    DataImportEventPayload eventPayload = eventPayloadRef.get();
    assertNotNull(eventPayload);

    JsonObject record = new JsonObject(eventPayload.getContext().get("MARC_BIBLIOGRAPHIC"));
    assertNotEquals(0, record.getInteger("order").intValue());
  }

  @Test
  public void shouldMarkJobExecutionAsErrorAndDeleteAllAssociatedRecords(TestContext testContext) {
    InitJobExecutionsRsDto response =
      constructAndPostInitJobExecutionRqDto(1);
    List<JobExecution> createdJobExecutions = response.getJobExecutions();
    assertThat(createdJobExecutions.size(), is(1));
    JobExecution jobExec = createdJobExecutions.get(0);

    WireMock.stubFor(post(RECORDS_SERVICE_URL)
      .willReturn(created().withTransformers(RequestToResponseTransformer.NAME)));
    WireMock.stubFor(WireMock.delete(new UrlPathPattern(new RegexPattern(SNAPSHOT_SERVICE_URL + "/.*"), true))
      .willReturn(WireMock.noContent()));

    Async async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .body(new JobProfileInfo()
        .withName("MARC records")
        .withId(DEFAULT_INSTANCE_JOB_PROFILE_ID)
        .withDataType(JobProfileInfo.DataType.MARC))
      .when()
      .put(JOB_EXECUTION_PATH + jobExec.getId() + JOB_PROFILE_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK);
    async.complete();

    async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .body(rawRecordsDto.withId(UUID.randomUUID().toString()))
      .when()
      .post(JOB_EXECUTION_PATH + jobExec.getId() + RECORDS_PATH)
      .then()
      .statusCode(HttpStatus.SC_NO_CONTENT);
    async.complete();

    async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .when()
      .delete(JOB_EXECUTION_PATH + jobExec.getId() + RECORDS_PATH)
      .then()
      .statusCode(HttpStatus.SC_NO_CONTENT);
    async.complete();

    async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .when()
      .get(JOB_EXECUTION_PATH + jobExec.getId())
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("status", is(CANCELLED.value()))
      .body("completedDate", notNullValue(Date.class));
    async.complete();
  }

  @Test
  public void shouldReturnNotFoundOnDeleteRecordsIfJobExecutionDoesNotExist(TestContext testContext) {
    Async async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .when()
      .delete(JOB_EXECUTION_PATH + UUID.randomUUID() + RECORDS_PATH)
      .then()
      .statusCode(HttpStatus.SC_NOT_FOUND);
    async.complete();
  }

  @Test
  public void shouldMarkJobExecutionAsErrorIfNoSourceChunksExist(TestContext testContext) {
    InitJobExecutionsRsDto response =
      constructAndPostInitJobExecutionRqDto(1);
    List<JobExecution> createdJobExecutions = response.getJobExecutions();
    assertThat(createdJobExecutions.size(), is(1));
    JobExecution jobExec = createdJobExecutions.get(0);

    WireMock.stubFor(WireMock.delete(new UrlPathPattern(new RegexPattern("/source-storage/snapshots/.{36}"), true))
      .willReturn(WireMock.noContent()));

    Async async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .body(new JobProfileInfo()
        .withName("MARC records")
        .withId(DEFAULT_INSTANCE_JOB_PROFILE_ID)
        .withDataType(JobProfileInfo.DataType.MARC))
      .when()
      .put(JOB_EXECUTION_PATH + jobExec.getId() + JOB_PROFILE_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK);
    async.complete();

    async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .when()
      .delete(JOB_EXECUTION_PATH + jobExec.getId() + RECORDS_PATH)
      .then()
      .statusCode(HttpStatus.SC_NO_CONTENT);
    async.complete();

    async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .when()
      .get(JOB_EXECUTION_PATH + jobExec.getId())
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("status", is(CANCELLED.value()))
      .body("completedDate", notNullValue(Date.class));
    async.complete();
  }

  @Test
  public void shouldMarkJobExecutionAsErrorAndDoNotDeleteRecordsIfDefaultJobProfileWasNotSet(TestContext testContext) {
    InitJobExecutionsRsDto response = constructAndPostInitJobExecutionRqDto(1);
    List<JobExecution> createdJobExecutions = response.getJobExecutions();
    assertThat(createdJobExecutions.size(), is(1));
    JobExecution jobExec = createdJobExecutions.get(0);

    WireMock.stubFor(post(RECORDS_SERVICE_URL)
      .willReturn(created().withTransformers(RequestToResponseTransformer.NAME)));

    Async async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .body(new JobProfileInfo()
        .withName("MARC records")
        .withId(JOB_PROFILE_ID)
        .withDataType(JobProfileInfo.DataType.MARC))
      .when()
      .put(JOB_EXECUTION_PATH + jobExec.getId() + JOB_PROFILE_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK);
    async.complete();

    async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .when()
      .delete(JOB_EXECUTION_PATH + jobExec.getId() + RECORDS_PATH)
      .then()
      .statusCode(HttpStatus.SC_NO_CONTENT);
    async.complete();

    async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .when()
      .get(JOB_EXECUTION_PATH + jobExec.getId())
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("status", is(CANCELLED.value()));
    async.complete();

    verify(0, deleteRequestedFor(new UrlPathPattern(new RegexPattern("/source-storage/snapshots/.{36}"), true)));
  }

  @Test
  public void shouldReturn204OkEventIfRemoveJobExecutionWithCommittedStatus(TestContext testContext) {

    InitJobExecutionsRsDto response =
      constructAndPostInitJobExecutionRqDto(1);
    List<JobExecution> createdJobExecutions = response.getJobExecutions();
    assertThat(createdJobExecutions.size(), is(1));
    JobExecution jobExec = createdJobExecutions.get(0);

    WireMock.stubFor(post(RECORDS_SERVICE_URL)
      .willReturn(created().withTransformers(RequestToResponseTransformer.NAME)));
    WireMock.stubFor(WireMock.delete(new UrlPathPattern(new RegexPattern(SNAPSHOT_SERVICE_URL + "/.*"), true))
      .willReturn(WireMock.noContent()));

    Async async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .body(new JobProfileInfo()
        .withName("MARC records")
        .withId(DEFAULT_INSTANCE_JOB_PROFILE_ID)
        .withDataType(JobProfileInfo.DataType.MARC))
      .when()
      .put(JOB_EXECUTION_PATH + jobExec.getId() + JOB_PROFILE_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK);
    async.complete();

    async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .body(rawRecordsDto.withId(UUID.randomUUID().toString()))
      .when()
      .post(JOB_EXECUTION_PATH + jobExec.getId() + RECORDS_PATH)
      .then()
      .statusCode(HttpStatus.SC_NO_CONTENT);
    async.complete();

    StatusDto status = new StatusDto().withStatus(COMMITTED);
    RestAssured.given()
      .spec(spec)
      .body(JsonObject.mapFrom(status).toString())
      .when()
      .put(JOB_EXECUTION_PATH + jobExec.getId() + STATUS_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK);

    async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .when()
      .delete(JOB_EXECUTION_PATH + jobExec.getId() + RECORDS_PATH)
      .then()
      .statusCode(HttpStatus.SC_NO_CONTENT);
    async.complete();

    async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .when()
      .get(JOB_EXECUTION_PATH + jobExec.getId())
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("status", is(COMMITTED.value()));
    async.complete();
  }

  @Test
  public void shouldProcessChunkOfRawRecordsWhenQueryParamIsFalse(TestContext testContext) {
    InitJobExecutionsRsDto response =
      constructAndPostInitJobExecutionRqDto(1);
    List<JobExecution> createdJobExecutions = response.getJobExecutions();
    assertThat(createdJobExecutions.size(), is(1));
    JobExecution jobExec = createdJobExecutions.get(0);

    WireMock.stubFor(post(RECORDS_SERVICE_URL)
      .willReturn(created().withTransformers(RequestToResponseTransformer.NAME)));

    Async async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .body(new JobProfileInfo()
        .withName("MARC records")
        .withId(DEFAULT_INSTANCE_JOB_PROFILE_ID)
        .withDataType(JobProfileInfo.DataType.MARC))
      .when()
      .put(JOB_EXECUTION_PATH + jobExec.getId() + JOB_PROFILE_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK);
    async.complete();

    rawRecordsDto.getRecordsMetadata().setLast(true);
    async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .body(rawRecordsDto.withId(UUID.randomUUID().toString()))
      .when()
      .post(JOB_EXECUTION_PATH + jobExec.getId() + RECORDS_PATH)
      .then()
      .statusCode(HttpStatus.SC_NO_CONTENT);
    async.complete();

    async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .when()
      .get(JOB_EXECUTION_PATH + jobExec.getId())
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("status", is(JobExecution.Status.PARSING_IN_PROGRESS.name()));
    async.complete();
  }

  @Test
  public void shouldNotOverride_999_ff_s_Subfield(TestContext testContext) throws InterruptedException {
    InitJobExecutionsRsDto response =
      constructAndPostInitJobExecutionRqDto(1);
    List<JobExecution> createdJobExecutions = response.getJobExecutions();
    assertThat(createdJobExecutions.size(), is(1));
    JobExecution jobExec = createdJobExecutions.get(0);

    WireMock.stubFor(post(new UrlPathPattern(new RegexPattern(PROFILE_SNAPSHOT_URL + "/.*"), true))
      .willReturn(WireMock.created().withBody(Json.encode(matchProfileSnapshotWrapperResponse))));

    WireMock.stubFor(get(new UrlPathPattern(new RegexPattern(PROFILE_SNAPSHOT_URL + "/.*"), true))
      .willReturn(WireMock.ok().withBody(Json.encode(matchProfileSnapshotWrapperResponse))));

    WireMock.stubFor(post(RECORDS_SERVICE_URL)
      .willReturn(created().withTransformers(RequestToResponseTransformer.NAME)));

    Async async = testContext.async();

    RestAssured.given()
      .spec(spec)
      .body(new JobProfileInfo()
        .withName("MARC records")
        .withId(DEFAULT_INSTANCE_JOB_PROFILE_ID)
        .withDataType(DataType.MARC))
      .when()
      .put(JOB_EXECUTION_PATH + jobExec.getId() + JOB_PROFILE_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK);
    async.complete();

    async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .body(rawRecordsDto_3)
      .when()
      .post(JOB_EXECUTION_PATH + jobExec.getId() + RECORDS_PATH)
      .then()
      .statusCode(HttpStatus.SC_NO_CONTENT);
    async.complete();

    String topicToObserve = formatToKafkaTopicName(DI_MARC_FOR_UPDATE_RECEIVED.value());
    List<String> observedValues = kafkaCluster.observeValues(ObserveKeyValues.on(topicToObserve, 1)
      .observeFor(2000, TimeUnit.SECONDS)
      .build());

    Event obtainedEvent = Json.decodeValue(observedValues.get(0), Event.class);
    assertEquals(DI_MARC_FOR_UPDATE_RECEIVED.value(), obtainedEvent.getEventType());
    DataImportEventPayload dataImportEventPayload = Json
      .decodeValue(obtainedEvent.getEventPayload(), DataImportEventPayload.class);
    Assert.assertNotNull(dataImportEventPayload.getContext());
    Assert.assertNotNull(dataImportEventPayload.getContext().get("MARC_BIBLIOGRAPHIC"));

    JsonObject record = new JsonObject(dataImportEventPayload.getContext().get("MARC_BIBLIOGRAPHIC"));
    Assert.assertNull(record.getString("matchedId"));
  }

  @Test
  public void shouldHaveErrorRecordIf999ffsFieldExistsAndCreateInstanceActionProfile(TestContext testContext)
    throws InterruptedException {
    InitJobExecutionsRsDto response =
      constructAndPostInitJobExecutionRqDto(1);
    List<JobExecution> createdJobExecutions = response.getJobExecutions();
    assertThat(createdJobExecutions.size(), is(1));
    JobExecution jobExec = createdJobExecutions.get(0);

    WireMock.stubFor(post(RECORDS_SERVICE_URL)
      .willReturn(created().withTransformers(RequestToResponseTransformer.NAME)));

    Async async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .body(new JobProfileInfo()
        .withName("MARC records")
        .withId(DEFAULT_INSTANCE_JOB_PROFILE_ID)
        .withDataType(DataType.MARC))
      .when()
      .put(JOB_EXECUTION_PATH + jobExec.getId() + JOB_PROFILE_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK);
    async.complete();

    async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .body(rawRecordsDto_3)
      .when()
      .post(JOB_EXECUTION_PATH + jobExec.getId() + RECORDS_PATH)
      .then()
      .statusCode(HttpStatus.SC_NO_CONTENT);
    async.complete();

    String topicToObserve = formatToKafkaTopicName(DI_ERROR.value());
    List<String> observedValues = kafkaCluster.observeValues(ObserveKeyValues.on(topicToObserve, 2)
      .observeFor(30, TimeUnit.SECONDS)
      .build());

    Event obtainedEvent = Json.decodeValue(observedValues.get(1), Event.class);
    assertEquals(DI_ERROR.value(), obtainedEvent.getEventType());
    DataImportEventPayload eventPayload = Json.decodeValue(obtainedEvent.getEventPayload(), DataImportEventPayload.class);

    assertNotNull(eventPayload.getContext());
    JsonObject record = new JsonObject(eventPayload.getContext().get("MARC_BIBLIOGRAPHIC"));
    assertNull(record.getString("matchedId"));
    assertFalse(record.getJsonObject("errorRecord").isEmpty());
    assertEquals("A new Instance was not created because the incoming record already contained a 999ff$s or 999ff$i field",
      new JsonObject(eventPayload.getContext().get("ERROR")).getString("error"));
  }

  @Test
  public void shouldHaveErrorRecordIsNullIf999ffsFieldExistsAndCreateInstanceActionProfileWithNonMatch(TestContext testContext) throws InterruptedException {
    InitJobExecutionsRsDto response = constructAndPostInitJobExecutionRqDto(1);
    List<JobExecution> createdJobExecutions = response.getJobExecutions();
    assertThat(createdJobExecutions.size(), is(1));
    JobExecution jobExec = createdJobExecutions.get(0);

    WireMock.stubFor(WireMock.get("/data-import-profiles/jobProfiles/" + DEFAULT_INSTANCE_JOB_PROFILE_ID + "?withRelations=false&")
      .willReturn(WireMock.ok().withBody(Json.encode(new JobProfile().withId(DEFAULT_INSTANCE_JOB_PROFILE_ID)
        .withName("Default - Create instance and SRS MARC Bib")))));

    WireMock.stubFor(post(RECORDS_SERVICE_URL)
      .willReturn(created().withTransformers(RequestToResponseTransformer.NAME)));
    WireMock.stubFor(post(new UrlPathPattern(new RegexPattern(PROFILE_SNAPSHOT_URL + "/.*"), true))
      .willReturn(WireMock.created().withBody(Json.encode(profileCreateMarcInstanceSnapshotWrapperResponse))));
    WireMock.stubFor(get(new UrlPathPattern(new RegexPattern(PROFILE_SNAPSHOT_URL + "/.*"), true))
      .willReturn(WireMock.ok().withBody(Json.encode(profileCreateMarcInstanceSnapshotWrapperResponse))));

    Async async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .body(new JobProfileInfo()
        .withName("MARC records")
        .withId(DEFAULT_MARC_HOLDINGS_JOB_PROFILE_ID)
        .withDataType(DataType.MARC))
      .when()
      .put(JOB_EXECUTION_PATH + jobExec.getId() + JOB_PROFILE_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK);
    async.complete();

    async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .body(rawRecordsDto_3)
      .when()
      .post(JOB_EXECUTION_PATH + jobExec.getId() + RECORDS_PATH)
      .then()
      .statusCode(HttpStatus.SC_NO_CONTENT);
    async.complete();

    String topicToObserve = formatToKafkaTopicName(DI_INCOMING_MARC_BIB_RECORD_PARSED.value());
    List<String> observedValues = kafkaCluster.observeValues(
      ObserveKeyValues.on(topicToObserve, 57).observeFor(30, TimeUnit.SECONDS).build());

    Event obtainedEvent = Json.decodeValue(observedValues.get(56), Event.class);
    assertEquals(DI_INCOMING_MARC_BIB_RECORD_PARSED.value(), obtainedEvent.getEventType());
    DataImportEventPayload eventPayload = Json.decodeValue(obtainedEvent.getEventPayload(), DataImportEventPayload.class);
    JsonObject record = new JsonObject(eventPayload.getContext().get("MARC_BIBLIOGRAPHIC"));
    assertNull(record.getString("matchedId"));
    assertNull(record.getJsonObject("errorRecord"));
  }

  @Test
  public void shouldHaveErrorRecordIf999ffsFieldExistsAndCreateMarcAuthorityActionProfile(TestContext testContext)
    throws InterruptedException {
    InitJobExecutionsRsDto response =
      constructAndPostInitJobExecutionRqDto(1);
    List<JobExecution> createdJobExecutions = response.getJobExecutions();
    assertThat(createdJobExecutions.size(), is(1));
    JobExecution jobExec = createdJobExecutions.get(0);

    WireMock.stubFor(WireMock.get("/data-import-profiles/jobProfiles/" + DEFAULT_MARC_AUTHORITY_JOB_PROFILE_ID + "?withRelations=false&")
      .willReturn(WireMock.ok().withBody(Json.encode(new JobProfile().withId(DEFAULT_MARC_AUTHORITY_JOB_PROFILE_ID).withName("Default - Create SRS MARC Authority")))));

    WireMock.stubFor(post(RECORDS_SERVICE_URL)
      .willReturn(created().withTransformers(RequestToResponseTransformer.NAME)));
    WireMock.stubFor(post(new UrlPathPattern(new RegexPattern(PROFILE_SNAPSHOT_URL + "/.*"), true))
      .willReturn(WireMock.created().withBody(Json.encode(profileMarcAuthoritySnapshotWrapperResponse))));
    WireMock.stubFor(get(new UrlPathPattern(new RegexPattern(PROFILE_SNAPSHOT_URL + "/.*"), true))
      .willReturn(WireMock.ok().withBody(Json.encode(profileMarcAuthoritySnapshotWrapperResponse))));

    Async async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .body(new JobProfileInfo()
        .withName("MARC records")
        .withId(DEFAULT_MARC_HOLDINGS_JOB_PROFILE_ID)
        .withDataType(DataType.MARC))
      .when()
      .put(JOB_EXECUTION_PATH + jobExec.getId() + JOB_PROFILE_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK);
    async.complete();

    async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .body(authorityRawRecordsDto)
      .when()
      .post(JOB_EXECUTION_PATH + jobExec.getId() + RECORDS_PATH)
      .then()
      .statusCode(HttpStatus.SC_NO_CONTENT);
    async.complete();

    String topicToObserve = formatToKafkaTopicName(DI_RAW_RECORDS_CHUNK_PARSED.value());
    List<String> observedValues = kafkaCluster.observeValues(ObserveKeyValues.on(topicToObserve, 1)
      .observeFor(30, TimeUnit.SECONDS)
      .build());

    Event obtainedEvent = Json.decodeValue(observedValues.get(0), Event.class);
    assertEquals(DI_RAW_RECORDS_CHUNK_PARSED.value(), obtainedEvent.getEventType());
    RecordCollection recordCollection = Json.decodeValue(obtainedEvent.getEventPayload(), RecordCollection.class);
    assertNull(recordCollection.getRecords().get(0).getMatchedId());
    assertNotNull(recordCollection.getRecords().get(0).getErrorRecord());
    assertEquals("{\"error\":\"A new MARC-Authority was not created because the incoming record already contained a 999ff$s or 999ff$i field\"}",
      recordCollection.getRecords().get(0).getErrorRecord().getDescription());
  }

  @Test
  public void shouldSetErrorToRecordWithInvalidLeaderLine(TestContext testContext) throws InterruptedException {
    InitJobExecutionsRsDto response =
      constructAndPostInitJobExecutionRqDto(1);
    List<JobExecution> createdJobExecutions = response.getJobExecutions();
    assertThat(createdJobExecutions.size(), is(1));
    JobExecution jobExec = createdJobExecutions.get(0);

    WireMock.stubFor(post(RECORDS_SERVICE_URL)
      .willReturn(created().withTransformers(RequestToResponseTransformer.NAME)));

    Async async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .body(new JobProfileInfo()
        .withName("MARC records")
        .withId(DEFAULT_INSTANCE_JOB_PROFILE_ID)
        .withDataType(DataType.MARC))
      .when()
      .put(JOB_EXECUTION_PATH + jobExec.getId() + JOB_PROFILE_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK);
    async.complete();

    async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .body(rawRecordsDto_4)
      .when()
      .post(JOB_EXECUTION_PATH + jobExec.getId() + RECORDS_PATH)
      .then()
      .statusCode(HttpStatus.SC_NO_CONTENT);
    async.complete();

    String topicToObserve = formatToKafkaTopicName(DI_ERROR.value());
    List<String> observedValues = kafkaCluster.observeValues(ObserveKeyValues.on(topicToObserve, 1)
      .observeFor(30, TimeUnit.SECONDS).build());

    Event obtainedEvent = Json.decodeValue(observedValues.get(0), Event.class);
    assertEquals(DI_ERROR.value(), obtainedEvent.getEventType());
    DataImportEventPayload eventPayload = Json.decodeValue(obtainedEvent.getEventPayload(), DataImportEventPayload.class);
    MatcherAssert.assertThat(new JsonObject(eventPayload.getContext().get("ERROR")).getString("message"),
      containsString("Error during analyze leader line for determining record type for record with id"));
  }

  @Test
  public void shouldHaveErrorRecordIf999ffsFieldExistsAndCreateMarcHoldingsActionProfile(TestContext testContext)
    throws InterruptedException {
    InitJobExecutionsRsDto response =
      constructAndPostInitJobExecutionRqDto(1);
    List<JobExecution> createdJobExecutions = response.getJobExecutions();
    assertThat(createdJobExecutions.size(), is(1));
    JobExecution jobExec = createdJobExecutions.get(0);

    WireMock.stubFor(WireMock.get("/data-import-profiles/jobProfiles/" + DEFAULT_MARC_HOLDINGS_JOB_PROFILE_ID + "?withRelations=false&")
      .willReturn(WireMock.ok().withBody(Json.encode(new JobProfile().withId(DEFAULT_MARC_HOLDINGS_JOB_PROFILE_ID).withName("Default - Create Holdings and SRS MARC Holdings")))));

    WireMock.stubFor(post(RECORDS_SERVICE_URL)
      .willReturn(created().withTransformers(RequestToResponseTransformer.NAME)));
    WireMock.stubFor(post(new UrlPathPattern(new RegexPattern(PROFILE_SNAPSHOT_URL + "/.*"), true))
      .willReturn(WireMock.created().withBody(Json.encode(profileCreateMarcHoldingsSnapshotWrapperResponse))));
    WireMock.stubFor(get(new UrlPathPattern(new RegexPattern(PROFILE_SNAPSHOT_URL + "/.*"), true))
      .willReturn(WireMock.ok().withBody(Json.encode(profileCreateMarcHoldingsSnapshotWrapperResponse))));

    Async async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .body(new JobProfileInfo()
        .withName("MARC records")
        .withId(DEFAULT_MARC_HOLDINGS_JOB_PROFILE_ID)
        .withDataType(DataType.MARC))
      .when()
      .put(JOB_EXECUTION_PATH + jobExec.getId() + JOB_PROFILE_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK);
    async.complete();

    async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .body(marcHoldingsRawRecordDto)
      .when()
      .post(JOB_EXECUTION_PATH + jobExec.getId() + RECORDS_PATH)
      .then()
      .statusCode(HttpStatus.SC_NO_CONTENT);
    async.complete();

    String topicToObserve = formatToKafkaTopicName(DI_RAW_RECORDS_CHUNK_PARSED.value());
    List<String> observedValues = kafkaCluster.observeValues(ObserveKeyValues.on(topicToObserve, 2)
      .observeFor(30, TimeUnit.SECONDS)
      .build());

    Event obtainedEvent = Json.decodeValue(observedValues.get(1), Event.class);
    assertEquals(DI_RAW_RECORDS_CHUNK_PARSED.value(), obtainedEvent.getEventType());
    RecordCollection recordCollection = Json.decodeValue(obtainedEvent.getEventPayload(), RecordCollection.class);
    assertNull(recordCollection.getRecords().get(0).getMatchedId());
    assertNotNull(recordCollection.getRecords().get(0).getErrorRecord());
    assertEquals("{\"error\":\"A new MARC-Holding was not created because the incoming record already contained a 999ff$s or 999ff$i field\"}",
      recordCollection.getRecords().get(0).getErrorRecord().getDescription());
  }

  @Test
  public void testDeleteChangeManagerJobExecutionsSingleEntity() {
    // given
    String jsonFiles;
    List<File> filesList;
    try {
      jsonFiles = TestUtil.readFileFromPath(FILES_PATH);
      filesList = new ObjectMapper().readValue(jsonFiles, new TypeReference<>() {
      });
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    List<File> limitedFilesList = filesList.stream().limit(1).collect(Collectors.toList());

    String stubUserId = UUID.randomUUID().toString();
    WireMock.stubFor(get(GET_USER_URL + stubUserId).willReturn(okJson(userResponse.toString())));
    InitJobExecutionsRqDto requestDto = new InitJobExecutionsRqDto();
    requestDto.getFiles().addAll(limitedFilesList);
    requestDto.setUserId(stubUserId);
    requestDto.setSourceType(InitJobExecutionsRqDto.SourceType.FILES);

    // when
    String parentJobExecutionId = RestAssured.given()
      .spec(spec)
      .body(JsonObject.mapFrom(requestDto).toString())
      .when().post(JOB_EXECUTION_PATH)
      .then().statusCode(HttpStatus.SC_CREATED)
      .extract().path("parentJobExecutionId");

    DeleteJobExecutionsReq deleteJobExecutionsReq =
      new DeleteJobExecutionsReq().withIds(Arrays.asList(parentJobExecutionId));
    RestAssured.given()
      .spec(spec)
      .body(deleteJobExecutionsReq)
      .when()
      .delete(JOB_EXECUTION_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("jobExecutionDetails.isDeleted.get(0)", is(true))
      .body("jobExecutionDetails.jobExecutionId.get(0)", is(parentJobExecutionId));
  }

  @Test
  public void testDeleteChangeManagerJobExecutionsMultipleIds() {
    // given
    String jsonFiles;
    List<File> filesList;
    try {
      jsonFiles = TestUtil.readFileFromPath(FILES_PATH);
      filesList = new ObjectMapper().readValue(jsonFiles, new TypeReference<>() {
      });
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    List<File> limitedFilesList = filesList.stream().limit(1).collect(Collectors.toList());

    String stubUserId = UUID.randomUUID().toString();
    WireMock.stubFor(get(GET_USER_URL + stubUserId).willReturn(okJson(userResponse.toString())));
    InitJobExecutionsRqDto requestDto = new InitJobExecutionsRqDto();
    requestDto.getFiles().addAll(limitedFilesList);
    requestDto.setUserId(stubUserId);
    requestDto.setSourceType(InitJobExecutionsRqDto.SourceType.FILES);

    // when
    String parentJobExecutionId = RestAssured.given()
      .spec(spec)
      .body(JsonObject.mapFrom(requestDto).toString())
      .when().post(JOB_EXECUTION_PATH)
      .then().statusCode(HttpStatus.SC_CREATED)
      .extract().path("parentJobExecutionId");

    // when
    String parentJobExecutionId_2 = RestAssured.given()
      .spec(spec)
      .body(JsonObject.mapFrom(requestDto).toString())
      .when().post(JOB_EXECUTION_PATH)
      .then().statusCode(HttpStatus.SC_CREATED)
      .extract().path("parentJobExecutionId");

    DeleteJobExecutionsReq deleteJobExecutionsReq =
      new DeleteJobExecutionsReq().withIds(Arrays.asList(parentJobExecutionId, parentJobExecutionId_2));
    RestAssured.given()
      .spec(spec)
      .body(deleteJobExecutionsReq)
      .when()
      .delete(JOB_EXECUTION_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("jobExecutionDetails.jobExecutionId.get(0)", is(parentJobExecutionId))
      .body("jobExecutionDetails.isDeleted.get(0)", is(true))
      .body("jobExecutionDetails.jobExecutionId.get(1)", is(parentJobExecutionId_2))
      .body("jobExecutionDetails.isDeleted.get(1)", is(true));
  }

  @Test
  public void testDeleteChangeManagerJobExecutionsFailure() {
    RestAssured.given()
      .spec(spec)
      .body("{\"ids\":null}")
      .when()
      .delete(JOB_EXECUTION_PATH)
      .then()
      .statusCode(HttpStatus.SC_INTERNAL_SERVER_ERROR);
  }

  @Test
  public void testDeleteChangeManagerJobExecutionsIncorrectBody() {
    RestAssured.given()
      .spec(spec)
      .body("{\"ids\":\"15065ccd-c305-4961-a411-8359e0b5a87b\"}")
      .when()
      .delete(JOB_EXECUTION_PATH)
      .then()
      .statusCode(HttpStatus.SC_BAD_REQUEST);
  }

  @Test
  public void testDeleteChangeManagerJobExecutionsDeletedIds() {
    String parentJobExecutionId = constructAndPostInitJobExecutionRqDto(1).getParentJobExecutionId();
    String parentJobExecutionId_2 = constructAndPostInitJobExecutionRqDto(1).getParentJobExecutionId();
    DeleteJobExecutionsResp deleteJobExecutionsResp =
      returnDeletedJobExecutionResponse(new String[] {parentJobExecutionId});
    assertThat(deleteJobExecutionsResp.getJobExecutionDetails().get(0).getJobExecutionId(), is(parentJobExecutionId));
    assertThat(deleteJobExecutionsResp.getJobExecutionDetails().get(0).getIsDeleted(), is(true));

    RestAssured.given()
      .spec(spec)
      .when()
      .get(JOB_EXECUTION_PATH + parentJobExecutionId)
      .then()
      .statusCode(HttpStatus.SC_NOT_FOUND);

    RestAssured.given()
      .spec(spec)
      .when()
      .get(JOB_EXECUTION_PATH + parentJobExecutionId_2)
      .then()
      .statusCode(HttpStatus.SC_OK);
  }

  public static DeleteJobExecutionsResp returnDeletedJobExecutionResponse(String[] parentJobExecutionId) {
    DeleteJobExecutionsReq deleteJobExecutionsReq =
      new DeleteJobExecutionsReq().withIds(Arrays.asList(parentJobExecutionId));
    return RestAssured.given()
      .spec(spec)
      .body(deleteJobExecutionsReq)
      .when()
      .delete(JOB_EXECUTION_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .extract().response().body().as(DeleteJobExecutionsResp.class);
  }

  @Test
  public void shouldNotReturnAnyChildrenOfAnyParentOnGetChildrenByIdForDeletedLogs() {
    InitJobExecutionsRsDto response =
      constructAndPostInitJobExecutionRqDto(25);
    List<JobExecution> createdJobExecutions = response.getJobExecutions();
    assertThat(createdJobExecutions.size(), is(26));
    JobExecution multipleParent = createdJobExecutions.stream()
      .filter(jobExec -> jobExec.getSubordinationType().equals(JobExecution.SubordinationType.PARENT_MULTIPLE))
      .findFirst().get();

    DeleteJobExecutionsResp deleteJobExecutionsResp =
      returnDeletedJobExecutionResponse(new String[] {multipleParent.getId()});
    assertThat(deleteJobExecutionsResp.getJobExecutionDetails().get(0).getJobExecutionId(), is(multipleParent.getId()));
    assertThat(deleteJobExecutionsResp.getJobExecutionDetails().get(0).getIsDeleted(), is(true));

    RestAssured.given()
      .spec(spec)
      .when()
      .get(JOB_EXECUTION_PATH + multipleParent.getId() + CHILDREN_PATH)
      .then()
      .statusCode(HttpStatus.SC_NOT_FOUND);
  }

  @Test
  public void shouldNotUpdateStatusOfDeletedRecords() {
    InitJobExecutionsRsDto response =
      constructAndPostInitJobExecutionRqDto(3);
    List<JobExecution> createdJobExecutions = response.getJobExecutions();
    assertThat(createdJobExecutions.size(), is(4));
    JobExecution child = createdJobExecutions.stream()
      .filter(jobExec -> jobExec.getSubordinationType().equals(JobExecution.SubordinationType.CHILD)).findFirst().get();

    DeleteJobExecutionsResp deleteJobExecutionsResp = returnDeletedJobExecutionResponse(new String[] {child.getId()});
    assertThat(deleteJobExecutionsResp.getJobExecutionDetails().get(0).getJobExecutionId(), is(child.getId()));
    assertThat(deleteJobExecutionsResp.getJobExecutionDetails().get(0).getIsDeleted(), is(true));

    StatusDto status = new StatusDto().withStatus(StatusDto.Status.PARSING_IN_PROGRESS);
    RestAssured.given()
      .spec(spec)
      .body(JsonObject.mapFrom(status).toString())
      .when()
      .put(JOB_EXECUTION_PATH + child.getId() + STATUS_PATH)
      .then()
      .statusCode(HttpStatus.SC_NOT_FOUND);
  }

  @Test
  public void shouldNotUpdateJobProfileOfDeletedRecords(TestContext testContext) {
    InitJobExecutionsRsDto response =
      constructAndPostInitJobExecutionRqDto(1);
    List<JobExecution> createdJobExecutions = response.getJobExecutions();
    assertThat(createdJobExecutions.size(), is(1));
    JobExecution jobExec = createdJobExecutions.get(0);

    DeleteJobExecutionsResp deleteJobExecutionsResp = returnDeletedJobExecutionResponse(new String[] {jobExec.getId()});
    assertThat(deleteJobExecutionsResp.getJobExecutionDetails().get(0).getJobExecutionId(), is(jobExec.getId()));
    assertThat(deleteJobExecutionsResp.getJobExecutionDetails().get(0).getIsDeleted(), is(true));

    Async async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .body(new JobProfileInfo()
        .withName("MARC records")
        .withId(DEFAULT_INSTANCE_JOB_PROFILE_ID)
        .withDataType(DataType.MARC))
      .when()
      .put(JOB_EXECUTION_PATH + jobExec.getId() + JOB_PROFILE_PATH)
      .then()
      .statusCode(HttpStatus.SC_NOT_FOUND);
    async.complete();
  }

  @Test
  public void shouldNotUpdateJobExecutionOfDeletedRecords() {
    InitJobExecutionsRsDto response =
      constructAndPostInitJobExecutionRqDto(1);
    List<JobExecution> createdJobExecutions = response.getJobExecutions();
    assertThat(createdJobExecutions.size(), is(1));
    JobExecution jobExec = createdJobExecutions.get(0);

    DeleteJobExecutionsResp deleteJobExecutionsResp = returnDeletedJobExecutionResponse(new String[] {jobExec.getId()});
    assertThat(deleteJobExecutionsResp.getJobExecutionDetails().get(0).getJobExecutionId(), is(jobExec.getId()));
    assertThat(deleteJobExecutionsResp.getJobExecutionDetails().get(0).getIsDeleted(), is(true));

    RestAssured.given()
      .spec(spec)
      .body(jobExec)
      .when()
      .put(JOB_EXECUTION_PATH + jobExec.getId())
      .then()
      .statusCode(HttpStatus.SC_NOT_FOUND);
  }
}
