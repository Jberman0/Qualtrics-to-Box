// STAI-State Scoring
Qualtrics.SurveyEngine.addOnPageSubmit(function() {
  // MUST ADD "STAI-State_ScoreRaw" AND "STAI-State_ScoreSummary" AS EMBEDDED DATA BEFORE THIS BLOCK
  
  // ── Configuration: Item mappings and settings ─────────
  const config = {
    reverseRows: [1, 2, 5, 8, 10, 11, 15, 16, 19, 20],  // Score as 1→4, 2→3, 3→2, 4→1
    anxietyLevels: [
      { max: 37, label: 'No or low anxiety' },
      { max: 44, label: 'Moderate anxiety' },
      { max: Infinity, label: 'High anxiety' }
    ]
  };
  
  // ── Scoring calculation ──────────────────────────────
  let totalScore = 0;
  const rows = this.getQuestionContainer().querySelectorAll('.ChoiceRow');
  
  rows.forEach((rowEl, index) => {
    const rowNum = index + 1;
    const selected = rowEl.querySelector('input[type="radio"]:checked');
    if (!selected) return;
    
    const responseValue = parseInt(selected.value, 10) || 0;

  if (config.reverseRows.includes(rowNum)) {
      totalScore += 5- responseValue;
    } else {
      totalScore += responseValue;
    }
  });
  
  // Determine anxiety level
  const scoreSummary = config.anxietyLevels.find(level => totalScore <= level.max).label;
  
  // ── Set embedded data ────────────────────────────────
  Qualtrics.SurveyEngine.setEmbeddedData("STAI-State_ScoreRaw", totalScore);
  Qualtrics.SurveyEngine.setEmbeddedData("STAI-State_ScoreSummary", scoreSummary);
});

// STAI-Trait Scoring
Qualtrics.SurveyEngine.addOnPageSubmit(function() {
  // MUST ADD "STAI-Trait_ScoreRaw" AND "STAI-Trait_ScoreSummary" AS EMBEDDED DATA BEFORE THIS BLOCK
  
  // ── Configuration: Item mappings and settings ─────────
  const config = {
    reverseRows: [1, 3, 6, 7, 10, 13, 14, 16, 19],  // Score as 1→4, 2→3, 3→2, 4→1
    anxietyLevels: [
      { max: 37, label: 'No or low anxiety' },
      { max: 44, label: 'Moderate anxiety' },
      { max: Infinity, label: 'High anxiety' }
    ]
  };
  
  // ── Scoring calculation ──────────────────────────────
  let totalScore = 0;
  const rows = this.getQuestionContainer().querySelectorAll('.ChoiceRow');
  
  rows.forEach((rowEl, index) => {
    const rowNum = index + 1;
    const selected = rowEl.querySelector('input[type="radio"]:checked');
    if (!selected) return;
    
    const responseValue = parseInt(selected.value, 10) || 0;
    
    if (config.reverseRows.includes(rowNum)) {
      totalScore += 5 - responseValue;
    } else {
      totalScore += responseValue;
    }
  });
  
  // Determine anxiety level
  const scoreSummary = config.anxietyLevels.find(level => totalScore <= level.max).label;
  
  // ── Set embedded data ────────────────────────────────
  Qualtrics.SurveyEngine.setEmbeddedData("STAI-Trait_ScoreRaw", totalScore);
  Qualtrics.SurveyEngine.setEmbeddedData("STAI-Trait_ScoreSummary", scoreSummary);

  // Save display order to participant
  const currentOrder = Qualtrics.SurveyEngine.getEmbeddedData("displayOrder");
  let updatedOrder = currentOrder ? currentOrder + ", " + "STAI" : "STAI";
  Qualtrics.SurveyEngine.setEmbeddedData("displayOrder", updatedOrder);
});