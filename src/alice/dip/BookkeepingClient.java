/*************
 * cil
 **************/

/*
 * This class is used to write the Dip information into the
 * Bookkeeping Data Base
 */
package alice.dip;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;

public class BookkeepingClient {
  private final HttpClient httpClient;
  private final String bookkeepingUrl;

  public BookkeepingClient(String bookkeepingUrl) {
    httpClient = HttpClient.newBuilder()
      .version(HttpClient.Version.HTTP_2)
      .connectTimeout(Duration.ofSeconds(10))
      .build();

    this.bookkeepingUrl = bookkeepingUrl;
  }

  public boolean doesFillExists(int fillNumber) {
    String getFillUrl = bookkeepingUrl + "/api/lhcFills/" + fillNumber;

    HttpRequest request = HttpRequest.newBuilder()
      .uri(URI.create(getFillUrl))
      .GET() // default
      .build();

    HttpResponse<String> response;

    try {
      response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

      if (response.statusCode() == 200) {
        String prob = "\"fillNumber\":" + fillNumber;
        String body = response.body();

        return body.contains(prob);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }

    return false;
  }

  public void createLhcFill(LhcInfoObj lhc) {
    boolean fillExists = doesFillExists(lhc.fillNo);

    if (fillExists) {
      AliDip2BK.log(3, "BKwriter.InserFill", "INSERT FILL ... BUT Fill No=" + lhc.fillNo + " is in BK ... trying to update record");
      updateLhcFill(lhc);
      return;
    }

    String furl = bookkeepingUrl + "/api/lhcFills";
    String requestBody = "{";
    requestBody = requestBody + "\n\"fillingSchemeName\":\"" + lhc.LHCFillingSchemeName + "\",";
    requestBody = requestBody + "\n\"beamType\":\"" + lhc.beamType + "\",";
    requestBody = requestBody + "\n\"fillNumber\":" + lhc.fillNo + ",";

    if (requestBody.endsWith(",")) {
      requestBody = requestBody.substring(0, requestBody.length() - 1);

    }
    requestBody = requestBody + "\n}";

    AliDip2BK.log(1, "BKwriter.InserFill", "FILL INSERT JSON request=\n" + requestBody);

    HttpRequest request = HttpRequest.newBuilder()
      .uri(URI.create(furl))
      .header("Content-Type", "application/json")
      .method("POST", HttpRequest.BodyPublishers.ofString(requestBody))
      .build();

    HttpResponse<String> response;

    try {
      response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
      AliDip2BK.log(2, "BKwriter.InserFill", " INSERT new FILL No=" + lhc.fillNo + "  Code=" + response.statusCode());
    } catch (Exception e) {
      AliDip2BK.log(4, "BKwriter.InserFill", "HTTP ERROR=" + e);
      e.printStackTrace();
    }
  }

  /*
   *  This method is used when new updates are received on the current Fill
   *  The modified values are updated in the DB
   */
  public void updateLhcFill(LhcInfoObj lhcFill) {
    boolean fillExists = doesFillExists(lhcFill.fillNo);

    if (!fillExists) {
      AliDip2BK.log(4, "BKwriter.UPdate FILL", "Fill No=" + lhcFill.fillNo + " is NOT in BK ");
      return;
    }

    String updateFillRequest = "{";

    long stableBeamStart = lhcFill.getStableBeamStart();
    if (stableBeamStart > 0) {
      updateFillRequest += "\n\"stableBeamsStart\":" + stableBeamStart + ",";
    }

    long stableBeamStop = lhcFill.getStableBeamStop();
    if (stableBeamStop > 0) {
      updateFillRequest += "\n\"stableBeamsEnd\":" + stableBeamStop + ",";
    }

    int stableBeamDuration = lhcFill.getStableBeamDuration();
    if (stableBeamDuration > 0) {
      updateFillRequest += "\n\"stableBeamsDuration\":" + stableBeamDuration + ",";
    }

    updateFillRequest += "\n\"fillingSchemeName\":\"" + lhcFill.LHCFillingSchemeName + "\",";

    if (updateFillRequest.endsWith(",")) {
      updateFillRequest = updateFillRequest.substring(0, updateFillRequest.length() - 1);

    }
    updateFillRequest = updateFillRequest + "\n}";

    AliDip2BK.log(1, "BKwriter.UpdateFILL", "UPDATE FILL=" + lhcFill.fillNo + " JSON request=\n" + updateFillRequest);

    String updateFillUrl = bookkeepingUrl + "/api/lhcFills/" + lhcFill.fillNo;

    HttpRequest request = HttpRequest.newBuilder()
      .uri(URI.create(updateFillUrl))
      .header("Content-Type", "application/json")
      .method("PATCH", HttpRequest.BodyPublishers.ofString(updateFillRequest))
      .build();

    HttpResponse<String> response;
    try {
      response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

      if ((response.statusCode() == 201)) {
        AliDip2BK.log(2, "BKwriter.UpdateFILL", "Succesful Update for FILL=" + lhcFill.fillNo);
      } else {
        AliDip2BK.log(3, "BKwriter.UpdateFILL", "ERROR for FILL=" + lhcFill.fillNo + " Code=" + +response.statusCode() + " Message=" + response.body());
      }
    } catch (Exception e) {
      AliDip2BK.log(4, "BKwriter.UpdateFILL", "ERROR Update for FILL=" + lhcFill.fillNo + "\n Exception=" + e);
      e.printStackTrace();
    }
  }

  public boolean doesRunExists(int runNumber) {
    // FIXME with the new filtering, fetching run number like 1234 will match if a run like `12345` exists
    String getRunsUrl = bookkeepingUrl + "/api/runs?filter[runNumbers]=" + runNumber;

    HttpRequest request = HttpRequest.newBuilder()
      .uri(URI.create(getRunsUrl))
      .GET() // default
      .build();

    HttpResponse<String> response;
    try {
      response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

      if (response.statusCode() == 200) {
        String prob = "\"runNumber\":" + runNumber;
        String body = response.body();

        return body.contains(prob);
      }

      AliDip2BK.log(3, "BKwriter.TestRunNo", " Reguest error =" + response.statusCode() + " Mesage=" + response.body());
    } catch (Exception e) {
      e.printStackTrace();
    }

    return false;
  }

  /*
   *  This method is used to update the RUN info entry
   */
  public void updateRun(RunInfoObj runObj) {
    boolean runExists;
    int retriesCounter = 0;
    do {
      runExists = doesRunExists(runObj.RunNo);
      if (!runExists) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException ex) {
          Thread.currentThread().interrupt();
        }
      }
    } while (++retriesCounter <= 10 && !runExists);

    if (retriesCounter > 0) AliDip2BK.log(1, "BKwriter.UpdateRun", "DELAY Loop Count=" + (retriesCounter));

    boolean hasModifications = false;

    String requestBody = "{";

    float beamEnergy = runObj.getBeamEnergy();
    if (beamEnergy > 0) {
      requestBody += "\n\"lhcBeamEnergy\":" + beamEnergy + ",";
      hasModifications = true;
    }

    String beamMode = runObj.getBeamMode();
    if (beamMode != null) {
      requestBody = requestBody + "\n\"lhcBeamMode\":\"" + beamMode + "\",";
      hasModifications = true;
    }

    float l3MagnetCurrent = runObj.getL3_magnetCurrent();
    if (l3MagnetCurrent < 0) l3MagnetCurrent = 0;
    if (l3MagnetCurrent >= 0) {
      requestBody += "\n\"aliceL3Current\":" + l3MagnetCurrent + ",";

      String l3MagnetPolarity = runObj.getL3_magnetPolarity();
      if ((l3MagnetPolarity.length() > 2) && (l3MagnetCurrent > 0)) {
        requestBody += "\n\"aliceL3Polarity\":\"" + l3MagnetPolarity + "\",";
      }

      hasModifications = true;
    }

    float dipoleMagnetCurrent = runObj.getDipole_magnetCurrent();
    if (dipoleMagnetCurrent < 0) dipoleMagnetCurrent = 0;
    if (dipoleMagnetCurrent >= 0) {
      requestBody += "\n\"aliceDipoleCurrent\":" + dipoleMagnetCurrent + ",";

      String dipoleMagnetPolarity = runObj.getDipole_magnetPolarity();
      if ((dipoleMagnetPolarity.length() > 2) && (dipoleMagnetCurrent > 0)) {
        requestBody += "\n\"aliceDipolePolarity\":\"" + dipoleMagnetPolarity + "\",";
      }

      hasModifications = true;
    }

    int fillNumber = runObj.getFillNo();
    if (fillNumber > 0) {
      requestBody += "\n\"fillNumber\":" + fillNumber + ",";
    }

    float betaStar = runObj.getLHCBetaStar();
    if (betaStar >= 0) {
      requestBody += "\n\"lhcBetaStar\":" + betaStar + ",";
      hasModifications = true;
    }

    if (!hasModifications) {  // no updates to be done !
      AliDip2BK.log(3, "BKwriter.UpdateRun", "No data to update for Run=" + runObj.RunNo);
      return;
    }

    if (requestBody.endsWith(",")) {
      requestBody = requestBody.substring(0, requestBody.length() - 1);

    }

    requestBody += "\n}";

    AliDip2BK.log(1, "BKwriter.UpdateRun", "RUN =" + runObj.RunNo + " UPDATE JSON request=\n" + requestBody);

    String patchRunRequest = bookkeepingUrl + "/api/runs?runNumber=" + runObj.RunNo;

    HttpRequest request = HttpRequest.newBuilder()
      .uri(URI.create(patchRunRequest))
      .header("Content-Type", "application/json")
      .method("PATCH", HttpRequest.BodyPublishers.ofString(requestBody))
      .build();

    HttpResponse<String> response;
    try {
      response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

      if (response.statusCode() == 200) {
        AliDip2BK.log(2, "BKwriter.UpdateRun", "Succesful Update for RUN=" + runObj.RunNo);
      } else {
        AliDip2BK.log(3, "BKwriter.UpdateRun", "ERROR for RUN=" + runObj.RunNo + " Code=" + +response.statusCode() + " Message=" + response.body());
      }

    } catch (Exception e) {
      AliDip2BK.log(4, "BKwriter.UpdateRun", "ERROR Update for RUN=" + runObj.RunNo + "\n Exception=" + e);
      e.printStackTrace();
    }
  }
}

