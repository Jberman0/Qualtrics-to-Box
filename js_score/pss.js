Qualtrics.SurveyEngine.addOnPageSubmit(function() {
  //  MUST ADD "PSS_ScoreRaw" & "PSS_ScoreSummary" AS EMBEDDED DATA BEFORE THE BLOCK:

  // ── Configuration ───────────────────────────────────────
  const config = {
    reverseItems: [4, 5, 7, 8],
    minScore: 0,
    maxScore: 4,
    severityLevels: [
      { max: 13, label: 'Low stress' },
      { max: 26, label: 'Moderate stress' },
      { max: Infinity, label: 'High stress' }
    ]
  };

  // ── Initialize ─────────────────────────────────────────
  const scores = { total: 0 };
  const rows = this.getQuestionContainer().querySelectorAll('.ChoiceRow');

  rows.forEach((rowEl, idx) => {
    const itemNum = idx + 1;
    const sel = rowEl.querySelector('input[type="radio"]:checked');
    if (!sel) return;

    let val = (parseInt(sel.value, 10) - 1) || 0;
    if (config.reverseItems.includes(itemNum)) {
      val = config.maxScore + config.minScore - val;
    }

    scores.total += val;
  });

  const totalSummary = config.severityLevels.find(level => scores.total <= level.max).label;

  // ── Set Embedded Data ─────────────────────────────────
  Qualtrics.SurveyEngine.setEmbeddedData("PSS_ScoreRaw", scores.total);
  Qualtrics.SurveyEngine.setEmbeddedData("PSS_ScoreSummary", totalSummary);

  // Save display order to participant
  const currentOrder = Qualtrics.SurveyEngine.getEmbeddedData("displayOrder");
  let updatedOrder = currentOrder ? currentOrder + ", " + "PSS" : "PSS";
  Qualtrics.SurveyEngine.setEmbeddedData("displayOrder", updatedOrder);
});