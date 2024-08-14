package alice.dip;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.stream.Collectors;

public class RunManager {
  private OptionalInt lastRunNumber = OptionalInt.empty();

  private final List<RunInfoObj> activeRuns = new ArrayList<>();

  public RunManager() {
  }

  public Optional<RunInfoObj> getRunByRunNumber(int runNumber) {
    if (activeRuns.isEmpty()) {
      return Optional.empty();
    }

    for (RunInfoObj run : activeRuns) {
      if (run.RunNo == runNumber) {
        return Optional.of(run);
      }
    }

    return Optional.empty();
  }

  public void addRun(RunInfoObj run) {
    activeRuns.add(run);
  }

  public void endRun(RunInfoObj run) {
    var removed = activeRuns.removeIf(activeRun -> run.RunNo == activeRun.RunNo);

    var currentRunIndex = OptionalInt.empty();
    for (int runIndex = 0; runIndex < activeRuns.size(); runIndex++) {
      if (activeRuns.get(runIndex).RunNo == run.RunNo) {
        currentRunIndex = OptionalInt.of(runIndex);
        break;
      }
    }

    if (currentRunIndex.isEmpty()) {
      AliDip2BK.log(4, "ProcData.EndRun", " ERROR RunNo=" + run.RunNo + " is not in the ACTIVE LIST ");
      statNoDuplicateEndRuns = statNoDuplicateEndRuns + 1;
    } else {
      statNoEndRuns = statNoEndRuns + 1;

      if (AliDip2BK.KEEP_RUNS_HISTORY_DIRECTORY != null) writeRunHistFile(run);

      if (AliDip2BK.SAVE_PARAMETERS_HISTORY_PER_RUN) {
        if (!run.energyHist.isEmpty()) {
          String fn = "Energy_" + run.RunNo + ".txt";
          writeHistFile(fn, run.energyHist);
        }

        if (!run.l3_magnetHist.isEmpty()) {
          String fn = "L3magnet_" + run.RunNo + ".txt";
          writeHistFile(fn, run.l3_magnetHist);
        }
      }

      activeRuns.remove(currentRunIndex.getAsInt());

      var activeRunsString = activeRuns.stream()
        .map(activeRun -> String.valueOf(activeRun.RunNo))
        .collect(Collectors.joining(", "));

      AliDip2BK.log(2, "ProcData.EndRun", " Correctly closed  runNo=" + run.RunNo
        + "  ActiveRuns size=" + activeRuns.size() + " " + activeRunsString);

      if (run.LHC_info_start.fillNo != run.LHC_info_stop.fillNo) {
        AliDip2BK.log(5, "ProcData.EndRun", " !!!! RUN =" + run.RunNo + "  Statred FillNo=" + run.LHC_info_start.fillNo + " and STOPED with FillNo=" + run.LHC_info_stop.fillNo);
      }
    }
  }

  public OptionalInt getLastRunNumber() {
    return lastRunNumber;
  }

  public void setLastRunNumber(int runNumber) {
    lastRunNumber = OptionalInt.of(runNumber);
  }
}
