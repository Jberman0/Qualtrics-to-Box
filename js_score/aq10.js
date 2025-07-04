Qualtrics.SurveyEngine.addOnPageSubmit(function() {
  // MUST ADD "AQ-10_ScoreRaw" & "AQ-10_ScoreSummary" AS EMBEDDED DATA BEFORE THIS BLOCK
  
  // ── Configuration: Item mappings and settings ─────────
  const config = {
    agreeRows: [1, 7, 8, 10],          // Items scored for Agree responses
    disagreeRows: [2, 3, 4, 5, 6, 9],  // Items scored for Disagree responses
    scoreThreshold: 6,                 // 6 or more = Significant
    agreeValues: [1, 2],               // "Definitely agree" and "Slightly agree"
    disagreeValues: [3, 4]             // "Slightly disagree" and "Definitely disagree"
  };
  
  // ── Scoring calculation ──────────────────────────────
  let totalScore = 0;
  const rows = this.getQuestionContainer().querySelectorAll('.ChoiceRow');
  
  rows.forEach((rowEl, index) => {
    const rowNum = index + 1;
    const selected = rowEl.querySelector('input[type="radio"]:checked');
    
    if (!selected) return; // Skip if no response selected
			   
    const responseValue = parseInt(selected.value, 10);
	
    // Score based on item type and response
    const isAgreeItem = config.agreeRows.includes(rowNum);
    const isDisagreeItem = config.disagreeRows.includes(rowNum);
    
    if (isAgreeItem && config.agreeValues.includes(responseValue)) {
      totalScore += 1;
    } else if (isDisagreeItem && config.disagreeValues.includes(responseValue)) {
      totalScore += 1;
    }
  });
  
  // Determine significance
  const scoreSummary = totalScore >= config.scoreThreshold ? "Significant" : "Not Significant";
  
  // ── Set embedded data ────────────────────────────────
  Qualtrics.SurveyEngine.setEmbeddedData("AQ-10_ScoreRaw", totalScore);
  Qualtrics.SurveyEngine.setEmbeddedData("AQ-10_ScoreSummary", scoreSummary);
});