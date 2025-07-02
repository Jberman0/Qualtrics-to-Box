Qualtrics.SurveyEngine.addOnPageSubmit(function() {
  //  MUST ADD THESE AS EMBEDDED DATA BEFORE THE BLOCK:
  // "CATI-Total_ScoreRaw",    "CATI-Total_ScoreSummary"
	
  //  Add thses as needed (for subscales):
  // "CATI-SOC_ScoreRaw",      "CATI-COM_ScoreRaw",
  // "CATI-CAM_ScoreRaw",      "CATI-RIG_ScoreRaw",
  // "CATI-REG_ScoreRaw",      "CATI-SEN_ScoreRaw"

  // ── Configuration ───────────────────────────────────────
  const config = {
    reverseItems: [8, 15, 19, 23, 28],
    itemSubscales: {
      SOC: [8, 10, 15, 17, 28, 30, 35],
      COM: [13, 19, 23, 26, 33, 37, 42],
      CAM: [3, 6, 9, 16, 22, 29, 39],
      RIG: [2, 5, 14, 21, 27, 34, 38],
      REG: [1, 7, 12, 20, 25, 32, 41],
      SEN: [4, 11, 18, 24, 31, 36, 40]
    },
    minScore: 1,
    maxScore: 5,
    totalThreshold: 134  // 134 or more = Significant
  };

  // ── Initialize ─────────────────────────────────────────
  let scores = { total: 0, SOC: 0, COM: 0, CAM: 0, RIG: 0, REG: 0, SEN: 0 };

  function getSubscale(itemNum) {
    for (const [sub, items] of Object.entries(config.itemSubscales)) {
      if (items.includes(itemNum)) return sub;
    }
    return null;
  }

  const rows = this.getQuestionContainer().querySelectorAll('.ChoiceRow');
  rows.forEach((rowEl, idx) => {
    const itemNum = idx + 1;
    const sel = rowEl.querySelector('input[type="radio"]:checked');
    if (!sel) return;

    let val = parseInt(sel.value, 10);
    if (config.reverseItems.includes(itemNum)) {
      val = config.maxScore + config.minScore - val;
    }

    scores.total += val;
    const sub = getSubscale(itemNum);
    if (sub) scores[sub] += val;
  });

  // ── Determine significance ──────────────────────────────
  const totalSummary = scores.total >= config.totalThreshold
    ? "Significant"
    : "Not Significant";

  // ── Set Embedded Data ───────────────────────────────────
  Qualtrics.SurveyEngine.setEmbeddedData("CATI-Total_ScoreRaw", scores.total);
  Qualtrics.SurveyEngine.setEmbeddedData("CATI-Total_ScoreSummary", totalSummary);

  Qualtrics.SurveyEngine.setEmbeddedData("CATI-SOC_ScoreRaw", scores.SOC);
  Qualtrics.SurveyEngine.setEmbeddedData("CATI-COM_ScoreRaw", scores.COM);
  Qualtrics.SurveyEngine.setEmbeddedData("CATI-CAM_ScoreRaw", scores.CAM);
  Qualtrics.SurveyEngine.setEmbeddedData("CATI-RIG_ScoreRaw", scores.RIG);
  Qualtrics.SurveyEngine.setEmbeddedData("CATI-REG_ScoreRaw", scores.REG);
  Qualtrics.SurveyEngine.setEmbeddedData("CATI-SEN_ScoreRaw", scores.SEN);
});