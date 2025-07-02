Qualtrics.SurveyEngine.addOnPageSubmit(function() {
  // MUST ADD "OCI-R_ScoreRaw" & "OCI-R_ScoreSummary" AS EMBEDDED DATA BEFORE THIS BLOCK

  // ── Scoring calculation ──────────────────────────────
  let totalScore = 0;
  let itemCount = 0;
  const rows = this.getQuestionContainer().querySelectorAll('.ChoiceRow');

  rows.forEach((rowEl, index) => {
    const rowNum = index + 1;
    const selected = rowEl.querySelector('input[type="radio"]:checked');

    if (!selected) {
      return;
    }

    const responseValue = (parseInt(selected.value, 10) - 1) || 0;
    totalScore += responseValue;
    itemCount++;
  });

	const scoreSummary = totalScore >= 21 ? "Significant" : "Not Significant";
	
  // ── Set embedded data ────────────────────────────────
  Qualtrics.SurveyEngine.setEmbeddedData("OCI-R_ScoreRaw", totalScore);
  Qualtrics.SurveyEngine.setEmbeddedData("OCI-R_ScoreSummary", scoreSummary);
});