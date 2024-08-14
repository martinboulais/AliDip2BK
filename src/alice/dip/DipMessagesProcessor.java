/*************
 * cil
 **************/

package alice.dip;

import cern.dip.DipData;
import cern.dip.DipTimestamp;

import java.io.*;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.OptionalInt;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;


/*
 * Process dip messages received from the DipClient
 * Receives DipData messages in a blocking Queue and then process them asynchronously
 * Creates Fill and Run data structures  to be stored in Alice bookkeeping system
 */
public class DipMessagesProcessor implements Runnable {
  private final BlockingQueue<MessageItem> outputQueue = new ArrayBlockingQueue<>(100);
  private final BookkeepingClient bookkeepingClient;
  private final RunManager runManager;

  public int statNoDipMess = 0;
  public int statNoKafMess = 0;
  public int statNoNewFills = 0;
  public int statNoNewRuns = 0;
  public int statNoEndRuns = 0;
  public int statNoDuplicateEndRuns = 0;

  private boolean acceptData = true;

  private LhcInfoObj currentFill = null;
  private final AliceInfoObj currentAlice;

  public DipMessagesProcessor(
    BookkeepingClient bookkeepingClient,
    RunManager runManager
  ) {
    this.bookkeepingClient = bookkeepingClient;
    this.runManager = runManager;

    Thread t = new Thread(this);
    t.start();

    currentAlice = new AliceInfoObj();
    loadState();
  }

  /*
   *  This method is used for receiving DipData messages from the Dip Client
   */
  synchronized public void handleMessage(String parameter, String message, DipData data) {
    if (!acceptData) {
      AliDip2BK.log(4, "ProcData.addData", " Queue is closed ! Data from " + parameter + " is NOT ACCEPTED");
      return;
    }

    MessageItem messageItem = new MessageItem(parameter, message, data);
    statNoDipMess = statNoDipMess + 1;

    try {
      outputQueue.put(messageItem);
    } catch (InterruptedException e) {
      AliDip2BK.log(4, "ProcData.addData", "ERROR adding new data ex= " + e);
      e.printStackTrace();
    }

    if (AliDip2BK.OUTPUT_FILE != null) {
      String file = AliDip2BK.ProgPath + AliDip2BK.OUTPUT_FILE;
      try {
        File of = new File(file);
        if (!of.exists()) {
          of.createNewFile();
        }
        BufferedWriter writer = new BufferedWriter(new FileWriter(file, true));


        writer.write("=>" + messageItem.format_message + "\n");
        writer.close();

      } catch (IOException e) {
        AliDip2BK.log(1, "ProcData.addData", "ERROR write data to dip output log  data ex= " + e);
      }
    }
  }

  // returns the length of the queue
  public int queueSize() {
    return outputQueue.size();
  }

  // used to stop the program;
  public void closeInputQueue() {
    acceptData = false;
  }

  @Override
  public void run() {
    while (true) {
      try {
        MessageItem messageItem = outputQueue.take();
        processNextInQueue(messageItem);
      } catch (InterruptedException e) {
        AliDip2BK.log(4, "ProcData.run", " Interrupt Error=" + e);
        e.printStackTrace();
      }
    }
  }

  /*
   *  This method is used to take appropriate action based on the Dip Data messages
   */
  public void processNextInQueue(MessageItem messageItem) {
    if (messageItem.param_name.contentEquals("dip/acc/LHC/RunControl/RunConfiguration")) {

      String ans = Util.parseDipMess(messageItem.param_name, messageItem.data);
      //	AliDip2BK.log(1, "ProcData.dispach->RunConf" ," new Dip mess from "+ " "+ ans  );
      try {
        String fillno = messageItem.data.extractString("FILL_NO");
        DipTimestamp dptime = messageItem.data.extractDipTime();
        long time = dptime.getAsMillis();

        String par1 = messageItem.data.extractString("PARTICLE_TYPE_B1");
        String par2 = messageItem.data.extractString("PARTICLE_TYPE_B2");
        String ais = messageItem.data.extractString("ACTIVE_INJECTION_SCHEME");
        String strIP2_NO_COLLISIONS = messageItem.data.extractString("IP2-NO-COLLISIONS");
        String strNO_BUNCHES = messageItem.data.extractString("NO_BUNCHES");


        AliDip2BK.log(1, "ProcData.dispach", " RunConfigurttion  FILL No = " + fillno + "  AIS=" + ais + " IP2_COLL=" + strIP2_NO_COLLISIONS);

        newFillNo(time, fillno, par1, par2, ais, strIP2_NO_COLLISIONS, strNO_BUNCHES);


      } catch (Exception e) {
        AliDip2BK.log(4, "ProcData.dispach", " ERROR in RunConfiguration P=" + messageItem.param_name + "  Ans=" + ans + " ex=" + e);

      }
      // SafeBeam
    } else if (messageItem.param_name.contentEquals("dip/acc/LHC/RunControl/SafeBeam")) {

      try {
        int v = messageItem.data.extractInt("payload");
        DipTimestamp dptime = messageItem.data.extractDipTime();
        long time = dptime.getAsMillis();

        newSafeMode(time, v);
      } catch (Exception e) {
        //AliDip2BK.log(1, "ProcData.dispach" ," ERROR on SafeBeam P="+ mes.param_name + " ex=" +e );

      }

      // Energy
    } else if (messageItem.param_name.contentEquals("dip/acc/LHC/Beam/Energy")) {
      try {
        int v = messageItem.data.extractInt("payload");
        DipTimestamp dptime = messageItem.data.extractDipTime();
        long time = dptime.getAsMillis();

        newEnergy(time, (float) (0.12 * (float) v));
      } catch (Exception e) {
        AliDip2BK.log(1, "ProcData.dispach", " ERROR on Energy P=" + messageItem.param_name + " ex=" + e);

      }
      // Beam Mode
    } else if (messageItem.param_name.contentEquals("dip/acc/LHC/RunControl/BeamMode")) {

      try {
        String v = messageItem.data.extractString("value");
        DipTimestamp dptime = messageItem.data.extractDipTime();
        long time = dptime.getAsMillis();

        AliDip2BK.log(1, "ProcData.dispach", " New Beam MOde = " + v);
        newBeamMode(time, v);


      } catch (Exception e) {
        AliDip2BK.log(3, "ProcData.dispach", " ERROR on Beam MOde on P=" + messageItem.param_name + " ex=" + e);
        //e.printStackTrace();
      }

    } else if (messageItem.param_name.contentEquals("dip/acc/LHC/Beam/BetaStar/Bstar2")) {
      try {
        int v = messageItem.data.extractInt("payload");
        DipTimestamp dptime = messageItem.data.extractDipTime();
        long time = dptime.getAsMillis();

        double v1 = v;

        double v2 = v1 / 1000.0; // in m

        newBetaStar(time, (float) v2);

      } catch (Exception e) {
        AliDip2BK.log(1, "ProcData.dispach", " ERROR on BetaStar  P=" + messageItem.param_name + " ex=" + e);
        //e.printStackTrace();
      }


    } else if (messageItem.param_name.contentEquals("dip/ALICE/MCS/Solenoid/Current")) {

      try {
        float v = messageItem.data.extractFloat();
        DipTimestamp dptime = messageItem.data.extractDipTime();
        long time = dptime.getAsMillis();
        newL3magnetCurrent(time, v);

      } catch (Exception e) {
        AliDip2BK.log(2, "ProcData.dispach", " ERROR on Solenoid Curr P=" + messageItem.param_name + " ex=" + e);

      }

    } else if (messageItem.param_name.contentEquals("dip/ALICE/MCS/Dipole/Current")) {

      try {
        float v = messageItem.data.extractFloat();
        DipTimestamp dptime = messageItem.data.extractDipTime();
        long time = dptime.getAsMillis();
        newDipoleCurrent(time, v);

      } catch (Exception e) {
        AliDip2BK.log(2, "ProcData.dispach", " ERROR on Dipole Curr on P=" + messageItem.param_name + " ex=" + e);

      }
    } else if (messageItem.param_name.contentEquals("dip/ALICE/MCS/Solenoid/Polarity")) {

      try {
        boolean v = messageItem.data.extractBoolean();
        //DipTimestamp dptime =mes.data.extractDipTime();
        //long time =dptime.getAsMillis();

        if (v) {
          currentAlice.L3_polarity = "Negative";
        } else {
          currentAlice.L3_polarity = "Positive";
        }

        AliDip2BK.log(2, "ProcData.dispach", " L3 Polarity=" + currentAlice.L3_polarity);

      } catch (Exception e) {
        AliDip2BK.log(2, "ProcData.dispach", " ERROR on L3 polarity P=" + messageItem.param_name + " ex=" + e);

      }
    } else if (messageItem.param_name.contentEquals("dip/ALICE/MCS/Dipole/Polarity")) {

      try {
        boolean v = messageItem.data.extractBoolean();
        //DipTimestamp dptime =mes.data.extractDipTime();
        //long time =dptime.getAsMillis();
        if (v)
          currentAlice.Dipole_polarity = "Negative";

        else {
          currentAlice.Dipole_polarity = "Positive";
        }

        AliDip2BK.log(2, "ProcData.dispach", " Dipole Polarity=" + currentAlice.Dipole_polarity);

      } catch (Exception e) {
        AliDip2BK.log(2, "ProcData.dispach", " ERROR on Dipole Polarity P=" + messageItem.param_name + " ex=" + e);
        //e.printStackTrace();
      }
    } else {
      AliDip2BK.log(4, "ProcData.dispach", "!!!!!!!!!! Unimplemented Data Process for P=" + messageItem.param_name);
    }

  }

  public void newSafeMode(long time, int val) {

    if (currentFill == null) return;

    String bm = currentFill.getBeamMode();

    if (bm.contentEquals("STABLE BEAMS")) {

      boolean isB1 = BigInteger.valueOf(val).testBit(0);
      boolean isB2 = BigInteger.valueOf(val).testBit(4);
      boolean isSB = BigInteger.valueOf(val).testBit(2);

      AliDip2BK.log(0, "ProcData.newSafeBeams", " VAL=" + val + " isB1=" + isB1 + " isB2=" + isB2 + " isSB=" + isSB);
      if (isB1 && isB2) {
        return;
      } else {

        currentFill.setBeamMode(time, "LOST BEAMS");
        AliDip2BK.log(5, "ProcData.newSafeBeams", " CHANGE BEAM MODE TO LOST BEAMS !!! ");
      }

      return;
    }

    if (bm.contentEquals("LOST BEAMS")) {

      boolean isB1 = BigInteger.valueOf(val).testBit(0);
      boolean isB2 = BigInteger.valueOf(val).testBit(4);
      //boolean isSB = BigInteger.valueOf(val).testBit(2);


      if (isB1 && isB2) {
        currentFill.setBeamMode(time, "STABLE BEAMS");
        AliDip2BK.log(5, "ProcData.newSafeBeams", " RECOVER FROM BEAM LOST TO STABLE BEAMS ");

      }

    }
  }

  public synchronized void newRunSignal(long date, int runNumber) {
    var run = runManager.getRunByRunNumber(runNumber);
    statNoNewRuns = statNoNewRuns + 1;
    statNoKafMess = statNoKafMess + 1;

    if (run.isEmpty()) {
      if (currentFill != null) {
        RunInfoObj newRun = new RunInfoObj(date, runNumber, currentFill.clone(), currentAlice.clone());
        runManager.addRun(newRun);
        AliDip2BK.log(2, "ProcData.newRunSignal", " NEW RUN NO =" + runNumber + "  with FillNo=" + currentFill.fillNo);
        bookkeepingClient.updateRun(newRun);

        var lastRunNumber = runManager.getLastRunNumber();

        // Check if there is the new run is right after the last one
        if (lastRunNumber.isPresent() && runNumber - lastRunNumber.getAsInt() != 1) {
          StringBuilder missingRunsList = new StringBuilder("<<");
          for (int missingRunNumber = (lastRunNumber.getAsInt() + 1); missingRunNumber < runNumber; missingRunNumber++) {
            missingRunsList.append(missingRunNumber).append(" ");
          }
          missingRunsList.append(">>");

          AliDip2BK.log(7, "ProcData.newRunSignal", " LOST RUN No Signal! " + missingRunsList + "  New RUN NO =" + runNumber + " Last Run No=" + lastRunNumber);
        }

        runManager.setLastRunNumber(runNumber);
      } else {
        RunInfoObj newRun = new RunInfoObj(date, runNumber, null, currentAlice.clone());
        runManager.addRun(newRun);
        AliDip2BK.log(2, "ProcData.newRunSignal", " NEW RUN NO =" + runNumber + " currentFILL is NULL Perhaps Cosmics Run");
        bookkeepingClient.updateRun(newRun);
      }
    } else {
      AliDip2BK.log(6, "ProcData.newRunSignal", " Duplicate new  RUN signal =" + runNumber + " IGNORE it");
    }
  }

  public synchronized void stopRunSignal(long time, int runNumber) {
    statNoKafMess = statNoKafMess + 1;

    var currentRun = runManager.getRunByRunNumber(runNumber);

    if (currentRun.isPresent()) {
      currentRun.get().setEORtime(time);
      if (currentFill != null) currentRun.get().LHC_info_stop = currentFill.clone();
      currentRun.get().alice_info_stop = currentAlice.clone();

      runManager.endRun(currentRun.get());
    } else {
      statNoDuplicateEndRuns = statNoDuplicateEndRuns + 1;
      AliDip2BK.log(4, "ProcData.stopRunSignal", " NO ACTIVE RUN having runNo=" + runNumber);
    }
  }

  public void newFillNo(long date, String strFno, String par1, String par2, String ais, String strIP2, String strNB) {
    int no = -1;
    int ip2Col = 0;
    int nob = 0;

    try {
      no = Integer.parseInt(strFno);
    } catch (NumberFormatException e1) {
      AliDip2BK.log(4, "ProcData.newFILL", "ERROR parse INT for fillNo= " + strFno);
      return;
    }

    try {
      ip2Col = Integer.parseInt(strIP2);
    } catch (NumberFormatException e1) {
      AliDip2BK.log(3, "ProcData.newFILL", "ERROR parse INT for IP2_COLLISIONS= " + strIP2);
    }

    try {
      nob = Integer.parseInt(strNB);
    } catch (NumberFormatException e1) {
      AliDip2BK.log(3, "ProcData.newFILL", "ERROR parse INT for NO_BUNCHES= " + strIP2);
    }


    if (currentFill == null) {
      currentFill = new LhcInfoObj(date, no, par1, par2, ais, ip2Col, nob);
      bookkeepingClient.createLhcFill(currentFill);
      saveState();
      AliDip2BK.log(2, "ProcData.newFillNo", " **CREATED new FILL no=" + no);
      statNoNewFills = statNoNewFills + 1;
      return;
    }
    if (currentFill.fillNo == no) { // the same fill no ;
      if (!ais.contains("no_value")) {
        boolean modi = currentFill.verifyAndUpdate(date, no, ais, ip2Col, nob);
        if (modi) {
          bookkeepingClient.updateLhcFill(currentFill);
          saveState();
          AliDip2BK.log(2, "ProcData.newFillNo", " * Update FILL no=" + no);
        }
      } else {
        AliDip2BK.log(4, "ProcData.newFillNo", " * FILL no=" + no + " AFS=" + ais);
      }
    } else {
      AliDip2BK.log(3, "ProcData.newFillNo", " Received new FILL no=" + no + "  BUT is an active FILL =" + currentFill.fillNo + " Close the old one and created the new one");
      currentFill.endedTime = (new Date()).getTime();
      if (AliDip2BK.KEEP_FILLS_HISTORY_DIRECTORY != null) {
        writeFillHistFile(currentFill);
      }
      bookkeepingClient.updateLhcFill(currentFill);

      currentFill = null;
      currentFill = new LhcInfoObj(date, no, par1, par2, ais, ip2Col, nob);
      bookkeepingClient.createLhcFill(currentFill);
      statNoNewFills = statNoNewFills + 1;
      saveState();
    }
  }

  public void newBeamMode(long date, String beamMode) {
    if (currentFill != null) {
      currentFill.setBeamMode(date, beamMode);

      int mc = -1;
      for (int i = 0; i < AliDip2BK.endFillCases.length; i++) {
        if (AliDip2BK.endFillCases[i].equalsIgnoreCase(beamMode)) mc = i;
      }
      if (mc < 0) {

        AliDip2BK.log(2, "ProcData.newBeamMode", "New beam mode=" + beamMode + "  for FILL_NO=" + currentFill.fillNo);
        bookkeepingClient.updateLhcFill(currentFill);
        saveState();

      } else {
        currentFill.endedTime = date;
        bookkeepingClient.updateLhcFill(currentFill);
        if (AliDip2BK.KEEP_FILLS_HISTORY_DIRECTORY != null) {
          writeFillHistFile(currentFill);
        }
        AliDip2BK.log(3, "ProcData.newBeamMode", "CLOSE Fill_NO=" + currentFill.fillNo + " Based on new  beam mode=" + beamMode);
        currentFill = null;
      }

    } else {
      AliDip2BK.log(4, "ProcData.newBeamMode", " ERROR new beam mode=" + beamMode + " NO FILL NO for it");

    }
  }


  public void newEnergy(long time, float v) {

    if (currentFill != null) {
      currentFill.setEnergy(time, v);
    }


    if (AliDip2BK.SAVE_PARAMETERS_HISTORY_PER_RUN) {
      if (activeRuns.size() == 0) return;

      for (int i = 0; i < activeRuns.size(); i++) {
        RunInfoObj r1 = activeRuns.get(i);
        r1.addEnergy(time, v);
      }
    }
  }

  public void newL3magnetCurrent(long time, float v) {

    if (currentAlice != null) {
      currentAlice.L3_magnetCurrent = v;
    }

    if (AliDip2BK.SAVE_PARAMETERS_HISTORY_PER_RUN) {
      if (activeRuns.size() == 0) return;

      for (int i = 0; i < activeRuns.size(); i++) {
        RunInfoObj r1 = activeRuns.get(i);
        r1.addL3_magnet(time, v);
      }
    }
  }

  public void newDipoleCurrent(long time, float v) {

    if (currentAlice != null) {
      currentAlice.Dipole_magnetCurrent = v;
    }

    if (AliDip2BK.SAVE_PARAMETERS_HISTORY_PER_RUN) {
      if (activeRuns.size() == 0) return;

      for (int i = 0; i < activeRuns.size(); i++) {
        RunInfoObj r1 = activeRuns.get(i);
        r1.addDipoleMagnet(time, v);
      }
    }

  }

  public void newBetaStar(long t, float v) {


    if (currentFill != null) {
      currentFill.setLHCBetaStar(t, v);
    }

  }

  public void saveState() {
    String path = getClass().getClassLoader().getResource(".").getPath();
    String full_file = path + AliDip2BK.KEEP_STATE_DIR + "/save_fill.jso";

    ObjectOutputStream oos = null;
    FileOutputStream fout = null;
    try {
      File of = new File(full_file);
      if (!of.exists()) {
        of.createNewFile();
      }
      fout = new FileOutputStream(full_file, false);
      oos = new ObjectOutputStream(fout);
      oos.writeObject(currentFill);
      oos.flush();
      oos.close();
    } catch (Exception ex) {
      AliDip2BK.log(4, "ProcData.saveState", " ERROR writing file=" + full_file + "   ex=" + ex);
      ex.printStackTrace();
    }


    String full_filetxt = path + AliDip2BK.KEEP_STATE_DIR + "/save_fill.txt";

    try {
      File of = new File(full_filetxt);
      if (!of.exists()) {
        of.createNewFile();
      }
      BufferedWriter writer = new BufferedWriter(new FileWriter(full_filetxt, false));
      String ans = currentFill.history();
      writer.write(ans);
      writer.close();
    } catch (IOException e) {

      AliDip2BK.log(4, "ProcData.saveState", " ERROR writing file=" + full_filetxt + "   ex=" + e);
    }
    AliDip2BK.log(2, "ProcData.saveState", " saved state for fill=" + currentFill.fillNo);
  }

  public void loadState() {
    String classPath = getClass().getClassLoader().getResource(".").getPath();
    String savedStatePath = classPath + AliDip2BK.KEEP_STATE_DIR + "/save_fill.jso";

    File savedStateFile = new File(savedStatePath);
    if (!savedStateFile.exists()) {
      AliDip2BK.log(2, "ProcData.loadState", " No Fill State file=" + savedStatePath);
      return;
    }

    try {
      var streamIn = new FileInputStream(savedStatePath);
      var objectinputstream = new ObjectInputStream(streamIn);
      LhcInfoObj savedLhcInfo = (LhcInfoObj) objectinputstream.readObject();
      objectinputstream.close();

      if (savedLhcInfo != null) {
        AliDip2BK.log(3, "ProcData.loadState", " Loaded sate for Fill =" + savedLhcInfo.fillNo);
        currentFill = savedLhcInfo;
      }
    } catch (Exception e) {
      AliDip2BK.log(4, "ProcData.loadState", " ERROR Loaded sate from file=" + savedStatePath);
      e.printStackTrace();
    }
  }

  public void writeFillHistFile(LhcInfoObj lhc) {
    String path = getClass().getClassLoader().getResource(".").getPath();

    String full_file = path + AliDip2BK.KEEP_FILLS_HISTORY_DIRECTORY + "/fill_" + lhc.fillNo + ".txt";

    try {
      File of = new File(full_file);
      if (!of.exists()) {
        of.createNewFile();
      }
      BufferedWriter writer = new BufferedWriter(new FileWriter(full_file, true));
      String ans = lhc.history();
      writer.write(ans);
      writer.close();
    } catch (IOException e) {

      AliDip2BK.log(4, "ProcData.writeFillHistFile", " ERROR writing file=" + full_file + "   ex=" + e);
    }
  }

  public void writeRunHistFile(RunInfoObj run) {
    String path = getClass().getClassLoader().getResource(".").getPath();
    String full_file = path + AliDip2BK.KEEP_RUNS_HISTORY_DIRECTORY + "/run_" + run.RunNo + ".txt";

    try {
      File of = new File(full_file);
      if (!of.exists()) {
        of.createNewFile();
      }
      BufferedWriter writer = new BufferedWriter(new FileWriter(full_file, true));
      String ans = run.toString();
      writer.write(ans);
      writer.close();
    } catch (IOException e) {

      AliDip2BK.log(4, "ProcData.writeRunHistFile", " ERROR writing file=" + full_file + "   ex=" + e);
    }
  }

  public void writeHistFile(String filename, ArrayList<TimestampedFloat> A) {

    String path = getClass().getClassLoader().getResource(".").getPath();
    String full_file = path + AliDip2BK.STORE_HIST_FILE_DIR + "/" + filename;

    try {
      File of = new File(full_file);
      if (!of.exists()) {
        of.createNewFile();
      }
      BufferedWriter writer = new BufferedWriter(new FileWriter(full_file, true));

      for (int i = 0; i < A.size(); i++) {
        TimestampedFloat ts = A.get(i);

        writer.write(ts.time + "," + ts.value + "\n");
      }
      writer.close();
    } catch (IOException e) {

      AliDip2BK.log(4, "ProcData.writeHistFile", " ERROR writing file=" + filename + "   ex=" + e);
    }
  }
}
