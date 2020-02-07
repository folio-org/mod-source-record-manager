package org.folio.rest.impl.changeManager;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.matching.RegexPattern;
import com.github.tomakehurst.wiremock.matching.UrlPathPattern;
import com.github.tomakehurst.wiremock.verification.LoggedRequest;
import io.restassured.RestAssured;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.http.HttpStatus;
import org.folio.TestUtil;
import org.folio.dao.JournalRecordDao;
import org.folio.dao.JournalRecordDaoImpl;
import org.folio.dao.util.PostgresClientFactory;
import org.folio.rest.impl.AbstractRestTest;
import org.folio.rest.jaxrs.model.File;
import org.folio.rest.jaxrs.model.InitJobExecutionsRqDto;
import org.folio.rest.jaxrs.model.InitJobExecutionsRsDto;
import org.folio.rest.jaxrs.model.InitialRecord;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JobProfileInfo;
import org.folio.rest.jaxrs.model.Progress;
import org.folio.rest.jaxrs.model.RawRecordsDto;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.RecordCollection;
import org.folio.rest.jaxrs.model.RecordsMetadata;
import org.folio.rest.jaxrs.model.RunBy;
import org.folio.rest.jaxrs.model.StatusDto;
import org.folio.services.Status;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.github.tomakehurst.wiremock.client.WireMock.created;
import static com.github.tomakehurst.wiremock.client.WireMock.findAll;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.putRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.serverError;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.urlMatching;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import static java.util.Arrays.asList;
import static javax.ws.rs.core.HttpHeaders.CONTENT_TYPE;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertEquals;

/**
 * REST tests for ChangeManager to manager JobExecution entities initialization
 */
@RunWith(VertxUnitRunner.class)
public class ChangeManagerAPITest extends AbstractRestTest {

  private static final String RECORDS_PATH = "/records";
  private static final String CHILDREN_PATH = "/children";
  private static final String STATUS_PATH = "/status";
  private static final String JOB_PROFILE_PATH = "/jobProfile";

  private static final String CORRECT_RAW_RECORD_1 = "01240cas a2200397   450000100070000000500170000700800410002401000170006502200140008203500260009603500220012203500110014403500190015504000440017405000150021808200110023322200420024424500430028626000470032926500380037630000150041431000220042932100250045136200230047657000290049965000330052865000450056165500420060670000450064885300180069386300230071190200160073490500210075094800370077195000340080836683220141106221425.0750907c19509999enkqr p       0   a0eng d  a   58020553   a0022-0469  a(CStRLIN)NYCX1604275S  a(NIC)notisABP6388  a366832  a(OCoLC)1604275  dCtYdMBTIdCtYdMBTIdNICdCStRLINdNIC0 aBR140b.J6  a270.0504aThe Journal of ecclesiastical history04aThe Journal of ecclesiastical history.  aLondon,bCambridge University Press [etc.]  a32 East 57th St., New York, 10022  av.b25 cm.  aQuarterly,b1970-  aSemiannual,b1950-690 av. 1-   Apr. 1950-  aEditor:   C. W. Dugmore. 0aChurch historyxPeriodicals. 7aChurch history2fast0(OCoLC)fst00860740 7aPeriodicals2fast0(OCoLC)fst014116411 aDugmore, C. W.q(Clifford William),eed.0381av.i(year)4081a1-49i1950-1998  apfndbLintz  a19890510120000.02 a20141106bmdbatcheltsxaddfast  lOLINaBR140b.J86h01/01/01 N01542ccm a2200361   ";
  private static final String CORRECT_RAW_RECORD_2 = "01314nam  22003851a 4500001001100000003000800011005001700019006001800036007001500054008004100069020003200110020003500142040002100177050002000198082001500218100002000233245008900253250001200342260004900354300002300403490002400426500002400450504006200474505009200536650003200628650001400660700002500674710001400699776004000713830001800753856009400771935001500865980003400880981001400914\u001Eybp7406411\u001ENhCcYBP\u001E20120404100627.6\u001Em||||||||d|||||||\u001Ecr||n|||||||||\u001E120329s2011    sz a    ob    001 0 eng d\u001E  \u001Fa2940447241 (electronic bk.)\u001E  \u001Fa9782940447244 (electronic bk.)\u001E  \u001FaNhCcYBP\u001FcNhCcYBP\u001E 4\u001FaZ246\u001Fb.A43 2011\u001E04\u001Fa686.22\u001F222\u001E1 \u001FaAmbrose, Gavin.\u001E14\u001FaThe fundamentals of typography\u001Fh[electronic resource] /\u001FcGavin Ambrose, Paul Harris.\u001E  \u001Fa2nd ed.\u001E  \u001FaLausanne ;\u001FaWorthing :\u001FbAVA Academia,\u001Fc2011.\u001E  \u001Fa1 online resource.\u001E1 \u001FaAVA Academia series\u001E  \u001FaPrevious ed.: 2006.\u001E  \u001FaIncludes bibliographical references (p. [200]) and index.\u001E0 \u001FaType and language -- A few basics -- Letterforms -- Words and paragraphs -- Using type.\u001E 0\u001FaGraphic design (Typography)\u001E 0\u001FaPrinting.\u001E1 \u001FaHarris, Paul,\u001Fd1971-\u001E2 \u001FaEBSCOhost\u001E  \u001FcOriginal\u001Fz9782940411764\u001Fz294041176X\u001E 0\u001FaAVA academia.\u001E40\u001Fuhttp://search.ebscohost.com/login.aspx?direct=true&scope=site&db=nlebk&db=nlabk&AN=430135\u001E  \u001Fa.o13465259\u001E  \u001Fa130307\u001Fb7107\u001Fe7107\u001Ff243965\u001Fg1\u001E  \u001FbOM\u001Fcnlnet\u001E\u001D\n";
  private static final String CORRECT_RAW_RECORD_3 = "03401nam  22004091i 4500001001200000003000800012005001700020006001800037007001500055008004100070020003200111020003500143020004300178020004000221040002100261050002700282082001900309245016200328300002300490500004700513504005100560505178700611588004702398650003702445650004502482650002902527650003602556700005502592700006102647710002302708730001902731776005902750856011902809935001502928980003402943981001402977\u001Eybp10134220\u001ENhCcYBP\u001E20130220102526.4\u001Em||||||||d|||||||\u001Ecr||n|||||||||\u001E130220s2013    ncu     ob    001 0 eng d\u001E  \u001Fa1476601852 (electronic bk.)\u001E  \u001Fa9781476601854 (electronic bk.)\u001E  \u001Fz9780786471140 (softcover : alk. paper)\u001E  \u001Fz078647114X (softcover : alk. paper)\u001E  \u001FaNhCcYBP\u001FcNhCcYBP\u001E 4\u001FaPN1995.9.V46\u001FbG37 2013\u001E04\u001Fa791.43/656\u001F223\u001E00\u001FaGame on, Hollywood!\u001Fh[electronic resource] :\u001Fbessays on the intersection of video games and cinema /\u001Fcedited by Gretchen Papazian and Joseph Michael Sommers.\u001E  \u001Fa1 online resource.\u001E  \u001FaDescription based on print version record.\u001E  \u001FaIncludes bibliographical references and index.\u001E0 \u001FaIntroduction: manifest narrativity-video games, movies, and art and adaptation / Gretchen Papazian and Joseph Michael Sommers -- The rules of engagement: watching, playing and other narrative processes. Playing the Buffyverse, playing the gothic: genre, gender and cross-media interactivity in Buffy the vampire slayer: chaos bleeds / Katrin Althans -- Dead eye: the spectacle of torture porn in Dead rising / Deborah Mellamphy -- Playing (with) the western: classical Hollywood genres in modern video games / Jason W. Buel -- Game-to-film adaptation and how Prince of Persia: the sands of time negotiates the difference between player and audience / Ben S. Bunting, Jr -- Translation between forms of interactivity: how to build the better adaptation / Marcus Schulzke -- The terms of the tale: time, place and other ideologically constructed conditions. -- Playing (in) the city: the warriors and images of urban disorder / Aubrey Anable -- When did Dante become a scythe-wielding badass? modeling adaption and shifting gender convention in Dante's Inferno / Denise A. Ayo -- \"Some of this happened to the other fellow\": remaking Goldeneye with Daniel Craig / David McGowan -- Zombie stripper geishas in the new global economy: racism and sexism in video games / Stewart Chang -- Stories, stories everywhere (and nowhere just the same): transmedia texts. \"My name is Alan Wake. I'm a writer.\": crafting narrative complexity in the age of transmedia storytelling / Michael Fuchs -- Millions of voices: Star wars, digital games, fictional worlds and franchise canon / Felan Parker -- The hype man as racial stereotype, parody and ghost in Afro samurai / Treaandrea M. Russworm -- Epic nostalgia: narrative play and transmedia storytelling in Disney epic Mickey / Lisa K. Dusenberry.\u001E  \u001FaDescription based on print version record.\u001E 0\u001FaMotion pictures and video games.\u001E 0\u001FaFilm adaptations\u001FxHistory and criticism.\u001E 0\u001FaVideo games\u001FxAuthorship.\u001E 0\u001FaConvergence (Telecommunication)\u001E1 \u001FaPapazian, Gretchen,\u001Fd1968-\u001Feeditor of compilation.\u001E1 \u001FaSommers, Joseph Michael,\u001Fd1976-\u001Feeditor of  compilation.\u001E2 \u001FaEbooks Corporation\u001E0 \u001FaEbook Library.\u001E08\u001FcOriginal\u001Fz9780786471140\u001Fz078647114X\u001Fw(DLC)  2012051432\u001E40\u001FzConnect to e-book on Ebook Library\u001Fuhttp://qut.eblib.com.au.AU/EBLWeb/patron/?target=patron&extendedid=P_1126326_0\u001E  \u001Fa.o13465405\u001E  \u001Fa130307\u001Fb6000\u001Fe6000\u001Ff243967\u001Fg1\u001E  \u001FbOM\u001Fcnlnet\u001E\u001D";
  private static final String RAW_RECORD_RESULTING_IN_PARSING_ERROR = "01247nam  2200313zu 450000100110000000300080001100500170001905\u001F222\u001E1 \u001FaAriáes, Philippe.\u001E10\u001FaWestern attitudes toward death\u001Fh[electronic resource] :\u001Fbfrom the Middle Ages to the present /\u001Fcby Philippe Ariáes ; translated by Patricia M. Ranum.\u001E  \u001FaJohn Hopkins Paperbacks ed.\u001E  \u001FaBaltimore :\u001FbJohns Hopkins University Press,\u001Fc1975.\u001E  \u001Fa1 online resource.\u001E1 \u001FaThe Johns Hopkins symposia in comparative history ;\u001Fv4th\u001E  \u001FaDescription based on online resource; title from digital title page (viewed on Mar. 7, 2013).\u001E 0\u001FaDeath.\u001E2 \u001FaEbrary.\u001E 0\u001FaJohns Hopkins symposia in comparative history ;\u001Fv4th.\u001E40\u001FzConnect to e-book on Ebrary\u001Fuhttp://gateway.library.qut.edu.au/login?url=http://site.ebrary.com/lib/qut/docDetail.action?docID=10635130\u001E  \u001Fa.o1346565x\u001E  \u001Fa130307\u001Fb2095\u001Fe2095\u001Ff243966\u001Fg1\u001E  \u001FbOM\u001Fcnlnet\u001E\u001D\n";
  private static final String RAW_RECORD_RESULTING_IN_INSTANCE_MAPPING_ERROR = "04466cas a2200841 a 4500001000700000005001700007008004100024010004500065012002400110019004000134022003900174029002100213029001700234029002200251030002700273032001700300035002300317035008400340035001100424040038000435042002300815049000900838050001600847060001400863074002400877082001200901086001500913222005400928245005400982246000901036264012701045264007101172300003901243310002601282321004801308321002501356336002601381337002801407338002701435362004101462490004601503490003701549500011301586500005901699500005401758500006201812525004001874530002401914555003501938650007801973650002602051650002802077650002002105710008402125710008802209760003202297760003102329770010302360776011802463780007402581830008102655830006802736856007102804850024102875856025603116891003503372891004003407891004403447891003803491891004303529891004003572994001203612\u001E130824\u001E20171107114428.0\u001E741101c19409999ncusr p      f0   a0eng  \u001E  \u001Fa   40029172 \u001Fzsn 79004523 \u001Fzsn 82020472 \u001E  \u001Fb3\u001Fi8709\u001Fj3\u001Fk1\u001Flg\u001Fm1\u001E  \u001Fa1588389\u001Fa2264617\u001Fa4342024\u001Fa14349066\u001E0 \u001Fa0027-8874\u001Fl0027-8874\u001Fz0198-0157\u001F21\u001E1 \u001FaNLGGC\u001Fb842279245\u001E1 \u001FaNZ1\u001Fb4973626\u001E1 \u001FaAU@\u001Fb000023028302\u001E  \u001FaJNCIEQ\u001FzJNCIAM\u001FzJJIND8\u001E  \u001Fa001964\u001FbUSPS\u001E  \u001Fa(OCoLC)ocm01064763\u001E  \u001Fa(OCoLC)1064763\u001Fz(OCoLC)1588389\u001Fz(OCoLC)2264617\u001Fz(OCoLC)4342024\u001Fz(OCoLC)14349066\u001E  \u001Fa130824\u001E  \u001FaNLM\u001FcNLM\u001FdNSD\u001FdDLC\u001FdNSD\u001FdOCL\u001FdIUL\u001FdNSD\u001FdOCL\u001FdGPO\u001FdNSD\u001FdGPO\u001FdRCS\u001FdIUL\u001FdAIP\u001FdNST\u001FdAIP\u001FdDLC\u001FdOCL\u001FdNST\u001FdGPO\u001FdNSD\u001FdNST\u001FdHUL\u001FdNST\u001FdHUL\u001FdGPO\u001FdNSD\u001FdNLM\u001FdOCL\u001FdNST\u001FdOCL\u001FdNSD\u001FdGPO\u001FdNST\u001FdNSD\u001FdNST\u001FdWAU\u001FdNLM\u001FdNSD\u001FdGPA\u001FdNSD\u001FdNST\u001FdNSD\u001FdOCL\u001FdNST\u001FdGPO\u001FdDLC\u001FdOCL\u001FdGPO\u001FdMYG\u001FdNSD\u001FdGPO\u001FdGUA\u001FdGPO\u001FdMYG\u001FdHLS\u001FdGPO\u001FdOCL\u001FdDLC\u001FdGPO\u001FdOCLCQ\u001FdGPO\u001FdNSD\u001FdOCLCQ\u001FdDLC\u001FdGUA\u001FdOCL\u001FdMYG\u001FdNLGGC\u001FdEEM\u001FdOCLCQ\u001FdUtOrBLW\u001E  \u001Fansdp\u001Fapcc\u001Fapremarc\u001E  \u001FaALMM\u001E00\u001FaRC261\u001Fb.U47\u001E00\u001FaW1 JO941C\u001E  \u001Fa0488, 0488 (online)\u001E10\u001Fa616\u001F211\u001E0 \u001FaHE 20.3161\u001E 0\u001FaJournal of the National Cancer Institute\u001Fb(Print)\u001E00\u001FaJournal of the National Cancer Institute :\u001FbJNCI.\u001E30\u001FaJNCI\u001E 1\u001Fa[Bethesda, Md.] :\u001FbU.S. Department of Health, Education, and Welfare, Public Health Service, National Institutes of Health\u001E 2\u001Fa[Washington, D.C.] :\u001Fb[For sale by the Supt. of Docs., U.S. G.P.O]\u001E  \u001Favolumes :\u001Fbillustrations ;\u001Fc28 cm.\u001E  \u001FaSemimonthly,\u001Fb<1989->\u001E  \u001FaBimonthly (irregular),\u001FbAug. 1940-Apr. 1956\u001E  \u001FaMonthly,\u001FbJune 1956-\u001E  \u001Fatext\u001Fbtxt\u001F2rdacontent\u001E  \u001Faunmediated\u001Fbn\u001F2rdamedia\u001E  \u001Favolume\u001Fbnc\u001F2rdacarrier\u001E1 \u001FaBegan with: v. 1, no. 1 (Aug. 1940).\u001E1 \u001Fa-Apr. 1979: DHEW publication ;\u001Fvno. (NIH)\u001E1 \u001FaMay 1979-<1996>: NIH publication\u001E  \u001FaNo longer an official government publication; no longer distributed to depository libraries after Dec. 2002.\u001E  \u001FaPublished: Cary, NC : Oxford University Press, <2003->\u001E  \u001FaDescription based on: Vol. 62, no. 1 (Jan. 1979).\u001E  \u001FaLatest issue consulted: Vol. 95, no. 14 (July 16, 2003->.\u001E  \u001FaSupplements accompany some numbers.\u001E  \u001FaAlso issued online.\u001E  \u001FaVols. 1 (1940)-16 (1956). 1 v.\u001E 0\u001FaCancer\u001FvPeriodicals.\u001F0http://id.loc.gov/authorities/subjects/sh2008100056\u001E 2\u001FaNeoplasms\u001FvAbstracts.\u001E 2\u001FaNeoplasms\u001FvPeriodicals.\u001E17\u001FaOncologie.\u001F2gtt\u001E2 \u001FaNational Cancer Institute (U.S.)\u001F0http://id.loc.gov/authorities/names/n79107940\u001E2 \u001FaNational Institutes of Health (U.S.)\u001F0http://id.loc.gov/authorities/names/n78085445\u001E0 \u001FtDHEW publication\u001Fx0090-0206\u001E0 \u001FtNIH publication\u001Fx0276-4733\u001E0 \u001FtJournal of the National Cancer Institute. Monographs\u001Fx1052-6773\u001Fw(DLC)   94660018\u001Fw(OCoLC)21986096\u001E08\u001FiOnline version:\u001FtJournal of the National Cancer Institute (Online)\u001Fx1460-2105\u001Fw(DLC)  2005233013\u001Fw(OCoLC)42465676\u001E05\u001FtCancer treatment reports\u001Fx0361-5960\u001Fw(DLC)   76645250\u001Fw(OCoLC)2101497\u001E 0\u001FaDHEW publication ;\u001Fvno. (NIH)\u001F0http://id.loc.gov/authorities/names/n42009070\u001E 0\u001FaNIH publication.\u001F0http://id.loc.gov/authorities/names/n84710198\u001E41\u001Fuhttp://www.oxfordjournals.org/content?genre=journal&issn=0027-8874\u001E  \u001FaAU\u001FaCSt\u001FaCSt-L\u001FaCU-AM\u001FaCU-I\u001FaCU-RivA\u001FaCU-SM\u001FaCaOTU\u001FaCaOWtU\u001FaCaQMU\u001FaCoFS\u001FaCtY-M\u001FaDLC\u001FaFU-HC\u001FaGU\u001FaIaU\u001FaInU\u001FaMBCo\u001FaMBU-M\u001FaMCM\u001FaMH-BL\u001FaMH-CS\u001FaMMeT\u001FaMSobPR\u001FaNSyU\u001FaNcRS\u001FaOU\u001FaPPPCPh\u001FaPPiC\u001FaPPT\u001FaPU\u001FaPU-Med\u001FaRPB-S\u001FaTU-M\u001FaTxHMC\u001FaULA\u001FaViRCU-H\u001FaWaU\u001E41\u001Fahttp://purl.access.gpo.gov/GPO/LPS1716\u001Fzback issues are accessible by clicking on \"ABOUT THE JOURNAL\" and then clicking on \"ARCHIVE\" at the bottom of the screen (some issues available in: Full Text and Abstracts, Abstracts and PDF, and Abstracts Only)\u001E12\u001F9853\u001F81\u001Fav.\u001Fbno.\u001Fu06\u001Fvr\u001Fi(year)\u001E40\u001F9863\u001F81\u001Fa<10>-\u001Fi<1949>-\u001Fxprovisional\u001E12\u001F9853\u001F82\u001Fav.\u001Fbno.\u001Fu20\u001Fvr\u001Fi(year)\u001Fj(month)\u001E40\u001F9863\u001F82\u001Fa<80>\u001Fi<1988>\u001Fxprovisional\u001E12\u001F9853\u001F83\u001Fav.\u001Fbno.\u001Fu24\u001Fvr\u001Fi(year)\u001Fj(date)\u001E40\u001F9863\u001F83\u001Fa<81>-\u001Fi<1989>-\u001Fxprovisional\u001E  \u001FaC0\u001FbALM\u001E\u001D";

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
    .withJobProfileInfo(new JobProfileInfo().withId(UUID.randomUUID().toString()).withName("Marc jobs profile"))
    .withUserId(UUID.randomUUID().toString());

  private RawRecordsDto rawRecordsDto = new RawRecordsDto()
    .withRecordsMetadata(new RecordsMetadata()
      .withLast(false)
      .withCounter(15)
      .withContentType(RecordsMetadata.ContentType.MARC_RAW))
    .withInitialRecords(Collections.singletonList(new InitialRecord().withRecord(CORRECT_RAW_RECORD_1))
    );

  @Spy
  private PostgresClientFactory postgresClientFactory = new PostgresClientFactory(Vertx.vertx());
  @InjectMocks
  private JournalRecordDao journalRecordDao = new JournalRecordDaoImpl();

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
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
      .withId(UUID.randomUUID().toString())
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
  public void testInitJobExecutionsWithNoFiles(TestContext context) {
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
  public void testInitJobExecutionsWithNoProfile(TestContext context) {
    // given
    InitJobExecutionsRqDto requestDto = new InitJobExecutionsRqDto();
    requestDto.setUserId(UUID.randomUUID().toString());
    requestDto.setSourceType(InitJobExecutionsRqDto.SourceType.ONLINE);

    // when
    RestAssured.given()
      .spec(spec)
      .body(JsonObject.mapFrom(requestDto).toString())
      .when().post(JOB_EXECUTION_PATH)
      .then().statusCode(HttpStatus.SC_BAD_REQUEST);
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
    Assert.assertThat(createdJobExecutions.size(), is(1));
    JobExecution singleParent = createdJobExecutions.get(0);
    Assert.assertThat(singleParent.getSubordinationType(), is(JobExecution.SubordinationType.PARENT_SINGLE));

    singleParent.setJobProfileInfo(new JobProfileInfo().withId(UUID.randomUUID().toString()).withName("Marc jobs profile"));
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
    Assert.assertThat(createdJobExecutions.size(), is(3));

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
    Assert.assertThat(createdJobExecutions.size(), is(26));
    JobExecution multipleParent = createdJobExecutions.stream()
      .filter(jobExec -> jobExec.getSubordinationType().equals(JobExecution.SubordinationType.PARENT_MULTIPLE)).findFirst().get();

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
    Assert.assertThat(createdJobExecutions.size(), is(26));
    JobExecution multipleParent = createdJobExecutions.stream()
      .filter(jobExec -> jobExec.getSubordinationType().equals(JobExecution.SubordinationType.PARENT_MULTIPLE)).findFirst().get();

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
  public void shouldReturnFilteredCollectionOnGetChildrenById() {
    int numberOfFiles = 25;
    int expectedNumberOfNew = 12;
    InitJobExecutionsRsDto response =
      constructAndPostInitJobExecutionRqDto(numberOfFiles);
    List<JobExecution> createdJobExecutions = response.getJobExecutions();
    Assert.assertThat(createdJobExecutions.size(), is(numberOfFiles + 1));
    JobExecution multipleParent = createdJobExecutions.stream()
      .filter(jobExec -> jobExec.getSubordinationType().equals(JobExecution.SubordinationType.PARENT_MULTIPLE)).findFirst().get();

    List<JobExecution> children = createdJobExecutions.stream()
      .filter(jobExec -> jobExec.getSubordinationType().equals(JobExecution.SubordinationType.CHILD)).collect(Collectors.toList());
    StatusDto parsingInProgressStatus = new StatusDto().withStatus(StatusDto.Status.PARSING_IN_PROGRESS);

    for (int i = 0; i < children.size() - expectedNumberOfNew; i++) {
      updateJobExecutionStatus(children.get(i), parsingInProgressStatus)
        .then()
        .statusCode(HttpStatus.SC_OK);
    }

    RestAssured.given()
      .spec(spec)
      .when()
      .get(JOB_EXECUTION_PATH + multipleParent.getId() + CHILDREN_PATH + "?query=status=" + StatusDto.Status.PARSING_IN_PROGRESS.name())
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("jobExecutions.size()", is(children.size() - expectedNumberOfNew))
      .body("totalRecords", is(children.size() - expectedNumberOfNew))
      .body("jobExecutions*.status", everyItem(is(JobExecution.Status.PARSING_IN_PROGRESS.name())))
      .body("jobExecutions*.subordinationType", everyItem(is(JobExecution.SubordinationType.CHILD.name())));
  }

  @Test
  public void shouldReturnEmptyCollectionOnGetChildrenByIdInCaseOfSingleParent() {
    InitJobExecutionsRsDto response =
      constructAndPostInitJobExecutionRqDto(1);
    List<JobExecution> createdJobExecutions = response.getJobExecutions();
    Assert.assertThat(createdJobExecutions.size(), is(1));
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
    Assert.assertThat(createdJobExecutions.size(), is(4));
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
    Assert.assertThat(createdJobExecutions.size(), is(1));
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
    Assert.assertThat(createdJobExecutions.size(), is(4));
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
  public void shouldSetCompletedDateToJobExecutionOnUpdateStatusToError() {
    InitJobExecutionsRsDto response =
      constructAndPostInitJobExecutionRqDto(1);
    List<JobExecution> createdJobExecutions = response.getJobExecutions();
    Assert.assertThat(createdJobExecutions.size(), is(1));
    JobExecution jobExecution = createdJobExecutions.get(0);
    Assert.assertThat(jobExecution.getSubordinationType(), is(JobExecution.SubordinationType.PARENT_SINGLE));
    Assert.assertNotNull(jobExecution.getRunBy().getFirstName());
    Assert.assertNotNull(jobExecution.getRunBy().getLastName());

    StatusDto status = new StatusDto().withStatus(StatusDto.Status.ERROR).withErrorStatus(StatusDto.ErrorStatus.FILE_PROCESSING_ERROR);
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
    Assert.assertThat(createdJobExecutions.size(), is(1));
    JobExecution jobExecution = createdJobExecutions.get(0);
    Assert.assertThat(jobExecution.getSubordinationType(), is(JobExecution.SubordinationType.PARENT_SINGLE));
    Assert.assertNotNull(jobExecution.getRunBy().getFirstName());
    Assert.assertNotNull(jobExecution.getRunBy().getLastName());

    StatusDto status = new StatusDto().withStatus(StatusDto.Status.ERROR).withErrorStatus(StatusDto.ErrorStatus.FILE_PROCESSING_ERROR);
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
    Assert.assertThat(createdJobExecutions.size(), is(1));
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
    Assert.assertThat(createdJobExecutions.size(), is(4));
    JobExecution parent = createdJobExecutions.stream()
      .filter(jobExec -> jobExec.getSubordinationType().equals(JobExecution.SubordinationType.PARENT_MULTIPLE)).findFirst().get();

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
    Assert.assertThat(createdJobExecutions.size(), is(3));
    JobExecution multipleParent = createdJobExecutions.stream()
      .filter(jobExec -> jobExec.getSubordinationType().equals(JobExecution.SubordinationType.PARENT_MULTIPLE)).findFirst().get();

    multipleParent.setJobProfileInfo(new JobProfileInfo().withId(UUID.randomUUID().toString()).withName("Marc jobs profile"));
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
    Assert.assertThat(createdJobExecutions.size(), is(3));
    JobExecution multipleParent = createdJobExecutions.stream()
      .filter(jobExec -> jobExec.getSubordinationType().equals(JobExecution.SubordinationType.PARENT_MULTIPLE)).findFirst().get();

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
    Assert.assertThat(createdJobExecutions.size(), is(1));
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
    JobProfileInfo jobProfile = new JobProfileInfo().withId(UUID.randomUUID().toString()).withName("marc");
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
    Assert.assertThat(createdJobExecutions.size(), is(1));
    JobExecution jobExec = createdJobExecutions.get(0);

    JobProfileInfo jobProfile = new JobProfileInfo().withId(UUID.randomUUID().toString()).withName("marc");
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
  public void shouldSetJobProfileInfoForJobExecution() {
    InitJobExecutionsRsDto response =
      constructAndPostInitJobExecutionRqDto(1);
    List<JobExecution> createdJobExecutions = response.getJobExecutions();
    Assert.assertThat(createdJobExecutions.size(), is(1));
    JobExecution jobExec = createdJobExecutions.get(0);

    JobProfileInfo jobProfile = new JobProfileInfo().withId(UUID.randomUUID().toString()).withName("marc");
    RestAssured.given()
      .spec(spec)
      .body(JsonObject.mapFrom(jobProfile).toString())
      .when()
      .put(JOB_EXECUTION_PATH + jobExec.getId() + JOB_PROFILE_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("jobProfileInfo.id", is(jobProfile.getId()))
      .body("jobProfileInfo.name", is(jobProfile.getName()))
      .body("jobProfileSnapshotWrapperId", is(profileSnapshotWrapperResponse.getId()));

    RestAssured.given()
      .spec(spec)
      .when()
      .get(JOB_EXECUTION_PATH + jobExec.getId())
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("jobProfileInfo.id", is(jobProfile.getId()))
      .body("jobProfileInfo.name", is(jobProfile.getName()))
      .body("jobProfileSnapshotWrapperId", is(profileSnapshotWrapperResponse.getId()));
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
      .statusCode(HttpStatus.SC_UNPROCESSABLE_ENTITY);
  }

  @Test
  public void shouldProcessChunkOfRawRecords(TestContext testContext) throws IOException {
    InitJobExecutionsRsDto response =
      constructAndPostInitJobExecutionRqDto(1);
    List<JobExecution> createdJobExecutions = response.getJobExecutions();
    Assert.assertThat(createdJobExecutions.size(), is(1));
    JobExecution jobExec = createdJobExecutions.get(0);

    WireMock.stubFor(post(INVENTORY_URL)
      .willReturn(created().withTransformers(RequestToResponseTransformer.NAME)));
    WireMock.stubFor(post(RECORDS_SERVICE_URL)
      .willReturn(created().withTransformers(RequestToResponseTransformer.NAME)));

    Async async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .body(new JobProfileInfo()
        .withName("MARC records")
        .withId(UUID.randomUUID().toString())
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

    String requestBody = findAll(putRequestedFor(urlEqualTo(PARSED_RECORDS_COLLECTION_URL))).get(0).getBodyAsString();
    RecordCollection recordCollection = new ObjectMapper().readValue(requestBody, RecordCollection.class);
    Assert.assertThat(recordCollection.getRecords().size(), is(not(0)));

    Record updatedRecord = recordCollection.getRecords().get(0);
    Assert.assertNotNull(updatedRecord.getExternalIdsHolder());
    Assert.assertNotNull(updatedRecord.getExternalIdsHolder().getInstanceId());

    async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .when()
      .get(JOB_EXECUTION_PATH + jobExec.getId())
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("status", is(JobExecution.Status.PARSING_IN_PROGRESS.name()))
      .body("runBy.firstName", is("DIKU"))
      .body("progress.total", is(15))
      .body("startedDate", notNullValue(Date.class)).log().all();
    async.complete();
  }

  @Test
  public void shouldProcess3ChunksAndRequestForMappingParameters1Time(TestContext testContext) {
    // given
    final int NUMBER_OF_CHUNKS = 3;

    InitJobExecutionsRsDto response = constructAndPostInitJobExecutionRqDto(1);
    List<JobExecution> createdJobExecutions = response.getJobExecutions();
    Assert.assertThat(createdJobExecutions.size(), is(1));
    JobExecution jobExec = createdJobExecutions.get(0);
    WireMock.stubFor(post(RECORDS_SERVICE_URL).willReturn(created().withTransformers(RequestToResponseTransformer.NAME)));

    Async async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .body(new JobProfileInfo()
        .withName("MARC records")
        .withId(UUID.randomUUID().toString())
        .withDataType(JobProfileInfo.DataType.MARC))
      .when()
      .put(JOB_EXECUTION_PATH + jobExec.getId() + JOB_PROFILE_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK);
    async.complete();

    // when
    for (int i = 0; i < NUMBER_OF_CHUNKS; i++) {
      async = testContext.async();
      RestAssured.given()
        .spec(spec)
        .body(rawRecordsDto)
        .when()
        .post(JOB_EXECUTION_PATH + jobExec.getId() + RECORDS_PATH)
        .then()
        .statusCode(HttpStatus.SC_NO_CONTENT);
      async.complete();
    }

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
      .body("progress.total", is(15))
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
    async.complete();
  }

  @Test
  public void shouldProcessChunkIfRequestForMappingParametersFails(TestContext testContext) {
    // given
    InitJobExecutionsRsDto response = constructAndPostInitJobExecutionRqDto(1);
    List<JobExecution> createdJobExecutions = response.getJobExecutions();
    Assert.assertThat(createdJobExecutions.size(), is(1));
    JobExecution jobExec = createdJobExecutions.get(0);
    WireMock.stubFor(post(RECORDS_SERVICE_URL).willReturn(created().withTransformers(RequestToResponseTransformer.NAME)));

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

    Async async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .body(new JobProfileInfo()
        .withName("MARC records")
        .withId(UUID.randomUUID().toString())
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
      .body("progress.total", is(15))
      .body("startedDate", notNullValue(Date.class)).log().all();

    async.complete();
  }


  @Test
  public void shouldParseChunkOfRawRecordsIfInstancesAreNotCreated() {
    InitJobExecutionsRsDto response =
      constructAndPostInitJobExecutionRqDto(1);
    List<JobExecution> createdJobExecutions = response.getJobExecutions();
    Assert.assertThat(createdJobExecutions.size(), is(1));
    JobExecution jobExec = createdJobExecutions.get(0);

    WireMock.stubFor(post(INVENTORY_URL)
      .willReturn(WireMock.serverError()));
    WireMock.stubFor(post(RECORDS_SERVICE_URL)
      .willReturn(created().withTransformers(RequestToResponseTransformer.NAME)));

    RestAssured.given()
      .spec(spec)
      .body(new JobProfileInfo()
        .withName("MARC records")
        .withId(UUID.randomUUID().toString())
        .withDataType(JobProfileInfo.DataType.MARC))
      .when()
      .put(JOB_EXECUTION_PATH + jobExec.getId() + JOB_PROFILE_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK);

    RestAssured.given()
      .spec(spec)
      .body(rawRecordsDto)
      .when()
      .post(JOB_EXECUTION_PATH + jobExec.getId() + RECORDS_PATH)
      .then()
      .statusCode(HttpStatus.SC_NO_CONTENT);
  }

  @Test
  public void shouldProcessChunkOfRawRecordsIfAddingAdditionalFieldsWasFail(TestContext testContext) {
    InitJobExecutionsRsDto response =
      constructAndPostInitJobExecutionRqDto(1);
    List<JobExecution> createdJobExecutions = response.getJobExecutions();
    Assert.assertThat(createdJobExecutions.size(), is(1));
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
        .withId(UUID.randomUUID().toString())
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
      .body("progress.total", is(15))
      .body("startedDate", notNullValue(Date.class)).log().all();
    async.complete();
  }

  @Test
  public void shouldNotParseChunkOfRawRecordsIfRecordListEmpty() {
    InitJobExecutionsRsDto response =
      constructAndPostInitJobExecutionRqDto(1);
    List<JobExecution> createdJobExecutions = response.getJobExecutions();
    Assert.assertThat(createdJobExecutions.size(), is(1));
    JobExecution jobExec = createdJobExecutions.get(0);

    WireMock.stubFor(post(INVENTORY_URL)
      .willReturn(WireMock.serverError()));

    WireMock.stubFor(post(RECORDS_SERVICE_URL)
      .willReturn(created().withTransformers(RequestToResponseTransformer.NAME)));

    RestAssured.given()
      .spec(spec)
      .body(new JobProfileInfo()
        .withName("MARC records")
        .withId(UUID.randomUUID().toString())
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
          .withCounter(1)))
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
    Assert.assertThat(createdJobExecutions.size(), is(1));
    JobExecution jobExec = createdJobExecutions.get(0);
    jobExec.setRunBy(new RunBy().withFirstName("DIKU").withLastName("ADMINISTRATOR"));
    jobExec.setProgress(new Progress().withCurrent(1000).withTotal(1000));
    jobExec.setStartedDate(new Date());

    WireMock.stubFor(post(RECORDS_SERVICE_URL)
      .willReturn(created().withTransformers(RequestToResponseTransformer.NAME)));

    WireMock.stubFor(post(INVENTORY_URL)
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
        .withId(UUID.randomUUID().toString())
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
      // status should be JobExecution.Status.PARSING_FINISHED but for first version we finish import in this place
      .body("status", is(JobExecution.Status.COMMITTED.name()));
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
    Assert.assertThat(createdJobExecutions.size(), is(1));
    JobExecution singleParent = createdJobExecutions.get(0);
    Assert.assertThat(singleParent.getSubordinationType(), is(JobExecution.SubordinationType.PARENT_SINGLE));

    singleParent.setJobProfileInfo(new JobProfileInfo().withId(UUID.randomUUID().toString()).withName("Marc jobs profile"));
    RestAssured.given()
      .spec(spec)
      .body(JsonObject.mapFrom(singleParent).toString())
      .when()
      .put(JOB_EXECUTION_PATH + singleParent.getId())
      .then()
      .statusCode(HttpStatus.SC_INTERNAL_SERVER_ERROR);
  }


  @Test
  public void shouldReturnErrorOnPostChunkOfRawRecordsWhenFailedPostRecordsToRecordsStorage() {
    InitJobExecutionsRsDto response =
      constructAndPostInitJobExecutionRqDto(1);
    List<JobExecution> createdJobExecutions = response.getJobExecutions();
    Assert.assertThat(createdJobExecutions.size(), is(1));
    JobExecution jobExec = createdJobExecutions.get(0);

    RestAssured.given()
      .spec(spec)
      .body(new JobProfileInfo()
        .withName("MARC records")
        .withId(UUID.randomUUID().toString())
        .withDataType(JobProfileInfo.DataType.MARC))
      .when()
      .put(JOB_EXECUTION_PATH + jobExec.getId() + JOB_PROFILE_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK);

    WireMock.stubFor(post(RECORDS_SERVICE_URL)
      .willReturn(WireMock.serverError()));

    RestAssured.given()
      .spec(spec)
      .body(rawRecordsDto)
      .when()
      .post(JOB_EXECUTION_PATH + jobExec.getId() + RECORDS_PATH)
      .then()
      .statusCode(HttpStatus.SC_INTERNAL_SERVER_ERROR);

    RestAssured.given()
      .spec(spec)
      .when()
      .get(JOB_EXECUTION_PATH + jobExec.getId())
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("status", is(JobExecution.Status.ERROR.name()));
  }

  @Test
  public void shouldReturnSavedParsedRecordsOnPostChunkOfRawRecordsWhenRecordsSavedPartially(TestContext testContext) {
    RawRecordsDto rawRecordsDto = new RawRecordsDto()
      .withRecordsMetadata(new RecordsMetadata()
        .withLast(false)
        .withCounter(15)
        .withContentType(RecordsMetadata.ContentType.MARC_RAW))
      .withInitialRecords(asList(new InitialRecord().withRecord(CORRECT_RAW_RECORD_1), new InitialRecord().withRecord(CORRECT_RAW_RECORD_1))
      );
    Async async = testContext.async();
    InitJobExecutionsRsDto response = constructAndPostInitJobExecutionRqDto(1);
    async.complete();
    List<JobExecution> createdJobExecutions = response.getJobExecutions();
    Assert.assertThat(createdJobExecutions.size(), is(1));
    JobExecution jobExec = createdJobExecutions.get(0);

    WireMock.stubFor(post(RECORDS_SERVICE_URL).willReturn(created().withTransformers(RequestToResponseTransformer.NAME)));
    WireMock.stubFor(post(INVENTORY_URL)
      .willReturn(created()//simulates partial success
        .withHeader(CONTENT_TYPE, APPLICATION_JSON)
        .withTransformers(InstancesBatchResponseTransformer.NAME)
      )
    );

    async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .body(new JobProfileInfo()
        .withName("MARC records")
        .withId(UUID.randomUUID().toString())
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
      .body("status", is(JobExecution.Status.PARSING_IN_PROGRESS.name()));
    async.complete();

    List<LoggedRequest> requests = findAll(putRequestedFor(urlMatching(PARSED_RECORDS_COLLECTION_URL)));

    assertEquals(1, requests.size());
    String body = requests.get(0).getBodyAsString();
    assertEquals(1, new JsonObject(body).getJsonArray("records").size());
  }

  @Test
  public void shouldReturnErrorOnPostJobExecutionWhenFailedPostSnapshotToStorage() throws IOException {
    WireMock.stubFor(post(SNAPSHOT_SERVICE_URL)
      .willReturn(WireMock.serverError()));

    InitJobExecutionsRqDto requestDto = new InitJobExecutionsRqDto();
    String jsonFiles = null;
    List<File> filesList = null;
    jsonFiles = TestUtil.readFileFromPath(FILES_PATH);
    filesList = new ObjectMapper().readValue(jsonFiles, new TypeReference<List<File>>() {
    });

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
  public void shouldProcessErrorRawRecords(TestContext testContext) {
    RawRecordsDto rawRecordsDtoContainingError = new RawRecordsDto()
      .withRecordsMetadata(new RecordsMetadata()
        .withLast(true)
        .withCounter(15)
        .withContentType(RecordsMetadata.ContentType.MARC_RAW))
      .withInitialRecords(asList(
        new InitialRecord().withRecord(CORRECT_RAW_RECORD_2),
        new InitialRecord().withRecord(RAW_RECORD_RESULTING_IN_PARSING_ERROR),
        new InitialRecord().withRecord(CORRECT_RAW_RECORD_3)));

    InitJobExecutionsRsDto response =
      constructAndPostInitJobExecutionRqDto(1);
    List<JobExecution> createdJobExecutions = response.getJobExecutions();
    Assert.assertThat(createdJobExecutions.size(), is(1));
    JobExecution jobExec = createdJobExecutions.get(0);
    jobExec.setRunBy(new RunBy().withFirstName("DIKU").withLastName("ADMINISTRATOR"));
    jobExec.setProgress(new Progress().withCurrent(1000).withTotal(1000));
    jobExec.setStartedDate(new Date());

    WireMock.stubFor(post(RECORDS_SERVICE_URL)
      .willReturn(created().withTransformers(RequestToResponseTransformer.NAME)));

    WireMock.stubFor(post(INVENTORY_URL)
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
        .withId(UUID.randomUUID().toString())
        .withDataType(JobProfileInfo.DataType.MARC))
      .when()
      .put(JOB_EXECUTION_PATH + jobExec.getId() + JOB_PROFILE_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK);
    async.complete();

    async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .body(rawRecordsDtoContainingError)
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
      // status should be JobExecution.Status.PARSING_FINISHED but for first version we finish import in this place
      .body("status", is(JobExecution.Status.COMMITTED.name()));
    async.complete();
  }

  @Test
  public void shouldMarkJobExecutionAsErrorIfInstancesWereNotCreated(TestContext testContext) {
    RawRecordsDto rawRecordsDtoContainingError = new RawRecordsDto()
      .withRecordsMetadata(new RecordsMetadata()
        .withLast(true)
        .withCounter(15)
        .withContentType(RecordsMetadata.ContentType.MARC_RAW))
      .withInitialRecords(Arrays.asList(
        new InitialRecord().withRecord(CORRECT_RAW_RECORD_2),
        new InitialRecord().withRecord(RAW_RECORD_RESULTING_IN_PARSING_ERROR),
        new InitialRecord().withRecord(CORRECT_RAW_RECORD_3)));

    InitJobExecutionsRsDto response =
      constructAndPostInitJobExecutionRqDto(1);
    List<JobExecution> createdJobExecutions = response.getJobExecutions();
    Assert.assertThat(createdJobExecutions.size(), is(1));
    JobExecution jobExec = createdJobExecutions.get(0);
    jobExec.setRunBy(new RunBy().withFirstName("DIKU").withLastName("ADMINISTRATOR"));
    jobExec.setProgress(new Progress().withCurrent(1000).withTotal(1000));
    jobExec.setStartedDate(new Date());

    WireMock.stubFor(post(INVENTORY_URL)
      .willReturn(WireMock.serverError()));

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
        .withId(UUID.randomUUID().toString())
        .withDataType(JobProfileInfo.DataType.MARC))
      .when()
      .put(JOB_EXECUTION_PATH + jobExec.getId() + JOB_PROFILE_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK);
    async.complete();

    async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .body(rawRecordsDtoContainingError)
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
      .body("status", is(JobExecution.Status.ERROR.name()))
      .body("completedDate", notNullValue(Date.class));
    async.complete();
  }

  @Test
  public void shouldMarkJobExecutionAsErrorAndSetCompletedDateWhenFailedPostRecordsToRecordsStorage() {
    RawRecordsDto lastRawRecordsDto = new RawRecordsDto()
      .withRecordsMetadata(new RecordsMetadata()
        .withLast(true)
        .withCounter(rawRecordsDto.getRecordsMetadata().getCounter())
        .withContentType(rawRecordsDto.getRecordsMetadata().getContentType()))
      .withInitialRecords(rawRecordsDto.getInitialRecords());

    InitJobExecutionsRsDto response =
      constructAndPostInitJobExecutionRqDto(1);
    List<JobExecution> createdJobExecutions = response.getJobExecutions();
    Assert.assertThat(createdJobExecutions.size(), is(1));
    JobExecution jobExec = createdJobExecutions.get(0);

    RestAssured.given()
      .spec(spec)
      .body(new JobProfileInfo()
        .withName("MARC records")
        .withId(UUID.randomUUID().toString())
        .withDataType(JobProfileInfo.DataType.MARC))
      .when()
      .put(JOB_EXECUTION_PATH + jobExec.getId() + JOB_PROFILE_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK);

    WireMock.stubFor(WireMock.post(RECORDS_SERVICE_URL)
      .willReturn(WireMock.serverError()));

    RestAssured.given()
      .spec(spec)
      .body(lastRawRecordsDto)
      .when()
      .post(JOB_EXECUTION_PATH + jobExec.getId() + RECORDS_PATH)
      .then()
      .statusCode(HttpStatus.SC_INTERNAL_SERVER_ERROR);

    RestAssured.given()
      .spec(spec)
      .when()
      .get(JOB_EXECUTION_PATH + jobExec.getId())
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("status", is(JobExecution.Status.ERROR.name()))
      .body("completedDate", notNullValue(Date.class));
  }

  @Test
  public void shouldProcessChunkOfRecordsAndFilterOutInvalidInstances(TestContext testContext) {
    RawRecordsDto rawRecordsDtoContainingError = new RawRecordsDto()
      .withRecordsMetadata(new RecordsMetadata()
        .withLast(true)
        .withCounter(15)
        .withContentType(RecordsMetadata.ContentType.MARC_RAW))
      .withInitialRecords(asList(
        new InitialRecord().withRecord(CORRECT_RAW_RECORD_2),
        new InitialRecord().withRecord(RAW_RECORD_RESULTING_IN_INSTANCE_MAPPING_ERROR),
        new InitialRecord().withRecord(CORRECT_RAW_RECORD_3)));

    InitJobExecutionsRsDto response =
      constructAndPostInitJobExecutionRqDto(1);
    List<JobExecution> createdJobExecutions = response.getJobExecutions();
    Assert.assertThat(createdJobExecutions.size(), is(1));
    JobExecution jobExec = createdJobExecutions.get(0);
    jobExec.setRunBy(new RunBy().withFirstName("DIKU").withLastName("ADMINISTRATOR"));
    jobExec.setProgress(new Progress().withCurrent(1000).withTotal(1000));
    jobExec.setStartedDate(new Date());

    WireMock.stubFor(post(RECORDS_SERVICE_URL)
      .willReturn(created().withTransformers(RequestToResponseTransformer.NAME)));

    WireMock.stubFor(post(INVENTORY_URL)
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
        .withId(UUID.randomUUID().toString())
        .withDataType(JobProfileInfo.DataType.MARC))
      .when()
      .put(JOB_EXECUTION_PATH + jobExec.getId() + JOB_PROFILE_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK);
    async.complete();

    async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .body(rawRecordsDtoContainingError)
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
      // status should be JobExecution.Status.PARSING_FINISHED but for first version we finish import in this place
      .body("status", is(JobExecution.Status.COMMITTED.name()));
    async.complete();

    List<LoggedRequest> requestsToInventory = findAll(postRequestedFor(urlMatching(INVENTORY_URL)));

    assertEquals(1, requestsToInventory.size());
    String requestBody = requestsToInventory.get(0).getBodyAsString();
    assertEquals(2, new JsonObject(requestBody).getJsonArray("instances").size());
  }

  @Test
  public void shouldProcessChunkOfRecordsAndFilterOutInvalidInstancesWhenRecordIsNotUpdated(TestContext testContext) {
    RawRecordsDto rawRecordsDtoContainingError = new RawRecordsDto()
      .withRecordsMetadata(new RecordsMetadata()
        .withLast(true)
        .withCounter(15)
        .withContentType(RecordsMetadata.ContentType.MARC_RAW))
      .withInitialRecords(asList(
        new InitialRecord().withRecord(CORRECT_RAW_RECORD_2),
        new InitialRecord().withRecord(RAW_RECORD_RESULTING_IN_INSTANCE_MAPPING_ERROR),
        new InitialRecord().withRecord(CORRECT_RAW_RECORD_3)));

    InitJobExecutionsRsDto response =
      constructAndPostInitJobExecutionRqDto(1);
    List<JobExecution> createdJobExecutions = response.getJobExecutions();
    Assert.assertThat(createdJobExecutions.size(), is(1));
    JobExecution jobExec = createdJobExecutions.get(0);
    jobExec.setRunBy(new RunBy().withFirstName("DIKU").withLastName("ADMINISTRATOR"));
    jobExec.setProgress(new Progress().withCurrent(1000).withTotal(1000));
    jobExec.setStartedDate(new Date());

    WireMock.stubFor(post(RECORDS_SERVICE_URL)
      .willReturn(created().withTransformers(RequestToResponseTransformer.NAME)));

    WireMock.stubFor(WireMock.put(new UrlPathPattern(new RegexPattern(RECORD_SERVICE_URL + "/.*"), true))
      .willReturn(WireMock.serverError()));

    WireMock.stubFor(post(INVENTORY_URL)
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
        .withId(UUID.randomUUID().toString())
        .withDataType(JobProfileInfo.DataType.MARC))
      .when()
      .put(JOB_EXECUTION_PATH + jobExec.getId() + JOB_PROFILE_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK);
    async.complete();

    async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .body(rawRecordsDtoContainingError)
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
      // status should be JobExecution.Status.PARSING_FINISHED but for first version we finish import in this place
      .body("status", is(JobExecution.Status.COMMITTED.name()));
    async.complete();

    List<LoggedRequest> requestsToInventory = findAll(postRequestedFor(urlMatching(INVENTORY_URL)));

    assertEquals(1, requestsToInventory.size());
    String requestBody = requestsToInventory.get(0).getBodyAsString();
    assertEquals(2, new JsonObject(requestBody).getJsonArray("instances").size());
  }

  @Test
  public void shouldDeleteJobExecutionAndAllAssociatedRecords(TestContext testContext) {
    InitJobExecutionsRsDto response =
      constructAndPostInitJobExecutionRqDto(1);
    List<JobExecution> createdJobExecutions = response.getJobExecutions();
    Assert.assertThat(createdJobExecutions.size(), is(1));
    JobExecution jobExec = createdJobExecutions.get(0);

    WireMock.stubFor(post(RECORDS_SERVICE_URL)
      .willReturn(created().withTransformers(RequestToResponseTransformer.NAME)));

    Async async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .body(new JobProfileInfo()
        .withName("MARC records")
        .withId(UUID.randomUUID().toString())
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
      .statusCode(HttpStatus.SC_NOT_FOUND);
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
  public void shouldNotDeleteJobExecutionIfRecordsWereNotDeleted(TestContext testContext) {
    InitJobExecutionsRsDto response =
      constructAndPostInitJobExecutionRqDto(1);
    List<JobExecution> createdJobExecutions = response.getJobExecutions();
    Assert.assertThat(createdJobExecutions.size(), is(1));
    JobExecution jobExec = createdJobExecutions.get(0);

    WireMock.stubFor(post(RECORDS_SERVICE_URL)
      .willReturn(created().withTransformers(RequestToResponseTransformer.NAME)));
    WireMock.stubFor(WireMock.delete(new UrlPathPattern(new RegexPattern("/source-storage/snapshots/.{36}/records"), true))
      .willReturn(WireMock.serverError()));

    Async async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .body(new JobProfileInfo()
        .withName("MARC records")
        .withId(UUID.randomUUID().toString())
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
      .delete(JOB_EXECUTION_PATH + jobExec.getId() + RECORDS_PATH)
      .then()
      .statusCode(HttpStatus.SC_INTERNAL_SERVER_ERROR);
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

  @Test
  public void shouldDeleteJobExecutionIfNoSourceChunksExist(TestContext testContext) {
    InitJobExecutionsRsDto response =
      constructAndPostInitJobExecutionRqDto(1);
    List<JobExecution> createdJobExecutions = response.getJobExecutions();
    Assert.assertThat(createdJobExecutions.size(), is(1));
    JobExecution jobExec = createdJobExecutions.get(0);

    Async async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .body(new JobProfileInfo()
        .withName("MARC records")
        .withId(UUID.randomUUID().toString())
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
      .statusCode(HttpStatus.SC_NOT_FOUND);
    async.complete();
  }

}
