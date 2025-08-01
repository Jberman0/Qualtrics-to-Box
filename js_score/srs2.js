Qualtrics.SurveyEngine.addOnPageSubmit(function() {
  // MUST ADD "SRS2_Total_TScore" AND "SRS2_ScoreSummary" AS EMBEDDED DATA BEFORE THIS BLOCK
	
  // Optional embedded data field below are for subscores - add as needed
  // T-score for subscales:
  // SRS2_SocialAwareness_TScore, SRS2_SocialCognition_TScore, SRS2_SocialCommunication_TScore,
  // SRS2_SocialMotivation_TScore, SRS2_RRB_TScore, SRS2_SCI_TScore
	
  // Raw scores for subscales and total score:
  // SRS2_SocialAwareness_ScoreRaw, SRS2_SocialCognition_ScoreRaw, SRS2_SocialCommunication_ScoreRaw,
  // SRS2_SocialMotivation_ScoreRaw, SRS2_RRB_ScoreRaw, SRS2_SCI_ScoreRaw, SRS2_Total_ScoreRaw

  // ── DEBUG CONFIGURATION ────────────────────────────────────
  const DEBUG_ENABLED = false; // Set to false to disable all debug logging
  
  function debugLog(message, data = null) {
    if (!DEBUG_ENABLED) return;
    
    if (data !== null) {
      console.log(`[SRS-2 DEBUG] ${message}:`, data);
    } else {
      console.log(`[SRS-2 DEBUG] ${message}`);
    }
  }
  
  function debugTable(label, data) {
    if (!DEBUG_ENABLED) return;
    console.log(`[SRS-2 DEBUG] ${label}:`);
    console.table(data);
  }
	
  // ── Configuration: Item mappings and lookup tables ─────────
  const config = {
    // Subscale mapping
    itemMappings: {
      socialAwareness: [2, 7, 25, 32, 45, 52, 54, 56],
      socialCognition: [5, 10, 15, 17, 30, 40, 42, 44, 48, 58, 59, 62],
      socialCommunication: [12, 13, 16, 18, 19, 21, 22, 26, 33, 35, 36, 37, 38, 41, 46, 47, 51, 53, 55, 57, 60, 61],
      socialMotivation: [1, 3, 6, 9, 11, 23, 27, 34, 43, 64, 65],
      rrb: [4, 8, 14, 20, 24, 28, 29, 31, 39, 49, 50, 63],
	  reverse: [3, 7, 11, 12, 15, 17, 21, 22, 26, 32, 38, 40, 43, 45, 48]
    },
    
    // Raw score to T-score conversion tables for each subscale and total
    // These lookup tables convert raw subscale scores to standardized T-scores
    lookupTables: {
      socialAwareness: {
        0:32, 1:35, 2:38, 3:41, 4:44, 5:47, 6:49, 7:52, 8:55, 9:58, 10:61, 
        11:64, 12:66, 13:69, 14:72, 15:75, 16:78, 17:81, 18:83, 19:86, 20:89
      },
      socialCognition: {
        0:37, 1:39, 2:41, 3:42, 4:44, 5:46, 6:48, 7:49, 8:51, 9:53, 10:55, 
        11:56, 12:58, 13:60, 14:62, 15:63, 16:65, 17:67, 18:69, 19:70, 20:72,
        21:74, 22:76, 23:77, 24:79, 25:81, 26:83, 27:84, 28:86, 29:88
      },
      socialCommunication: {
        0:37, 1:38, 2:39, 3:40, 4:41, 5:42, 6:43, 7:44, 8:45, 9:46, 10:47,
        11:48, 12:49, 13:50, 14:51, 15:52, 16:53, 17:54, 18:55, 19:56, 20:57,
        21:58, 22:59, 23:60, 24:61, 25:62, 26:63, 27:64, 28:65, 29:66, 30:67,
        31:68, 32:69, 33:70, 34:71, 35:72, 36:73, 37:74, 38:75, 39:75, 40:76,
        41:77, 42:78, 43:79, 44:80, 45:81, 46:82, 47:83, 48:84, 49:85, 50:86,
        51:87, 52:88, 53:89
      },
      socialMotivation: {
        0:37, 1:39, 2:41, 3:42, 4:44, 5:46, 6:47, 7:49, 8:51, 9:52, 10:54, 
        11:56, 12:57, 13:59, 14:61, 15:62, 16:64, 17:66, 18:67, 19:69, 20:71,
        21:72, 22:74, 23:76, 24:77, 25:79, 26:81, 27:82, 28:84, 29:86, 30:87, 31:89
      },
      rrb: {
        0:40, 1:42, 2:43, 3:45, 4:47, 5:48, 6:50, 7:52, 8:53, 9:55, 10:57,
        11:58, 12:60, 13:62, 14:63, 15:65, 16:67, 17:68, 18:70, 19:72, 20:73,
        21:75, 22:77, 23:78, 24:80, 25:82, 26:83, 27:85, 28:87, 29:88
      },
      sci: {
        0:35, 1:36, 2:36, 3:37, 4:37, 5:37, 6:38, 7:38, 8:39, 9:39, 10:40,
        11:40, 12:40, 13:41, 14:41, 15:42, 16:42, 17:43, 18:43, 19:43, 20:44,
        21:44, 22:45, 23:45, 24:46, 25:46, 26:46, 27:47, 28:47, 29:48, 30:48,
        31:49, 32:49, 33:49, 34:50, 35:50, 36:51, 37:51, 38:52, 39:52, 40:53,
        41:53, 42:53, 43:54, 44:54, 45:55, 46:55, 47:56, 48:56, 49:56, 50:57,
        51:57, 52:58, 53:58, 54:59, 55:59, 56:59, 57:60, 58:60, 59:61, 60:61,
        61:62, 62:62, 63:62, 64:63, 65:63, 66:64, 67:64, 68:65, 69:65, 70:65,
        71:66, 72:66, 73:67, 74:67, 75:68, 76:68, 77:68, 78:69, 79:69, 80:70,
        81:70, 82:71, 83:71, 84:71, 85:72, 86:72, 87:73, 88:73, 89:74, 90:74,
        91:74, 92:75, 93:75, 94:76, 95:76, 96:77, 97:77, 98:78, 99:78, 100:78,
        101:79, 102:79, 103:80, 104:80, 105:81, 106:81, 107:81, 108:82, 109:82, 
        110:83, 111:83, 112:84, 113:84, 114:84, 115:85, 116:85, 117:86, 118:86, 
        119:87, 120:87, 121:87, 122:88, 123:88, 124:89, 125:89
      },
      total: {
        0: 36, 1: 36, 2: 36, 3: 37, 4: 37, 5: 38, 6: 38, 7: 38, 8: 39, 9: 39, 10: 39,
        11: 40, 12: 40, 13: 40, 14: 41, 15: 41, 16: 41, 17: 42, 18: 42, 19: 42, 20: 43,
        21: 43, 22: 44, 23: 44, 24: 44, 25: 45, 26: 45, 27: 45, 28: 46, 29: 46, 30: 46,
        31: 47, 32: 47, 33: 47, 34: 48, 35: 48, 36: 48, 37: 49, 38: 49, 39: 50, 40: 50,
        41: 50, 42: 51, 43: 51, 44: 51, 45: 51, 46: 51, 47: 51, 48: 53, 49: 53, 50: 53, 
        51: 54, 52: 54, 53: 54, 54: 55, 55: 55, 56: 55, 57: 56, 58: 56, 59: 57, 60: 57, 
        61: 57, 62: 58, 63: 58, 64: 58, 65: 59, 66: 59, 67: 59, 68: 60, 69: 60, 70: 60, 
        71: 61, 72: 61, 73: 61, 74: 62, 75: 62, 76: 63, 77: 63, 78: 63, 79: 64, 80: 64,
        81: 64, 82: 65, 83: 65, 84: 65, 85: 66, 86: 66, 87: 66, 88: 67, 89: 67, 90: 67, 
        91: 68, 92: 68, 93: 69, 94: 69, 95: 69, 96: 70, 97: 70, 98: 70, 99: 71, 100: 71, 
        101: 71, 102: 72, 103: 72, 104: 72, 105: 73, 106: 73, 107: 73, 108: 74, 109: 74, 
        110: 75, 111: 75, 112: 75, 113: 76, 114: 76, 115: 76, 116: 77, 117: 77, 118: 77, 
        119: 78, 120: 78, 121: 78, 122: 79, 123: 79, 124: 79, 125: 80, 126: 80, 127: 80, 
        128: 81, 129: 81, 130: 82, 131: 82, 132: 82, 133: 83, 134: 83, 135: 83, 136: 84, 
        137: 84, 138: 84, 139: 85, 140: 85, 141: 85, 142: 86, 143: 86, 144: 86, 145: 87, 
        146: 87, 147: 88, 148: 88, 149: 88, 150: 89, 151: 89, 152: 89
      }
    }
  };

  debugLog("SRS-2 scoring calculation started");
  debugLog("Debug mode enabled", { debugEnabled: DEBUG_ENABLED });

  // ── Helper functions ────────────────────────────────────────
  // Converts raw score to T-score using lookup table, defaults to 90 if not found
  function getTScore(rawScore, subscale) {
    const tScore = rawScore in config.lookupTables[subscale] 
      ? config.lookupTables[subscale][rawScore] 
      : 90;
    
    debugLog(`T-Score lookup for ${subscale}`, { rawScore, tScore, defaultUsed: !(rawScore in config.lookupTables[subscale]) });
    return tScore;
  }

  // Determines severity level based on total T-score using standard SRS-2 cutoffs
  function getSeverityLevel(tScore) {
    let level;
    if (tScore <= 59) level = 'Within Normal Limits';
    else if (tScore <= 65) level = 'Mild Range';
    else if (tScore <= 75) level = 'Moderate Range';
    else level = 'Severe Range';
    
    debugLog("Severity level determination", { tScore, level });
    return level;
  }

  // Wrapper function for setting Qualtrics embedded data
  function setEmbeddedData(name, value) {
    debugLog(`Setting embedded data: ${name}`, value);
    Qualtrics.SurveyEngine.setEmbeddedData(name, value);
  }

  // ── Calculate raw scores ───────────────────────────────────
  const rawScores = {
    socialAwareness: 0,
    socialCognition: 0,
    socialCommunication: 0,
    socialMotivation: 0,
    rrb: 0,
    total: 0
  };

  debugLog("Starting raw score calculation");
  debugLog("Item mappings", config.itemMappings);

  // Process each survey response row
  const rows = this.getQuestionContainer().querySelectorAll('.ChoiceRow');
  debugLog(`Found ${rows.length} survey rows`);
  
  const itemResponses = []; // For debugging individual responses
  
  rows.forEach((rowEl, index) => {
    const selectedInput = rowEl.querySelector('input[type="radio"]:checked');
    const itemNumber = index + 1;
    
    if (!selectedInput) {
      debugLog(`No response for item ${itemNumber}`);
      itemResponses.push({ item: itemNumber, response: null, subscale: 'none' });
      return;
    }

	let value;  
    if (config.itemMappings.reverse.includes(itemNumber)) {
      value = (3 - (parseInt(selectedInput.value, 10) - 1)) || 0;
    } else {
      value = (parseInt(selectedInput.value, 10) - 1) || 0;
    }
      // Find which subscale this item belongs to
      let assignedSubscale = 'unknown';
      for (const [subscale, items] of Object.entries(config.itemMappings)) {
      if (subscale === 'reverse') continue; // Skip reverse mapping
      if (items.includes(itemNumber)) {
        rawScores[subscale] += value;
        assignedSubscale = subscale;
        break;
      }
    }
    
    rawScores.total += value;
    
    itemResponses.push({
      item: itemNumber,
      response: value,
      subscale: assignedSubscale
    });
    
    debugLog(`Item ${itemNumber}: response=${value}, subscale=${assignedSubscale}`);
  });

  // Calculate SCI (Social Communication & Interaction) composite score
  rawScores.sci = rawScores.socialAwareness + rawScores.socialCognition + 
                  rawScores.socialCommunication + rawScores.socialMotivation;

  debugLog("Raw scores calculated");
  debugTable("Raw Scores Summary", rawScores);
  debugTable("Individual Item Responses", itemResponses);

  // ── Calculate T-scores ─────────────────────────────────────
  debugLog("Starting T-score calculations");
  const tScores = {};
  for (const subscale of Object.keys(rawScores)) {
    tScores[subscale] = getTScore(rawScores[subscale], subscale);
  }

  debugLog("T-scores calculated");
  debugTable("T-Scores Summary", tScores);

  // ── Set embedded data ──────────────────────────────────────
  debugLog("Setting embedded data fields");
  
  const fieldMappings = {
    // Raw scores - store the summed raw scores for each subscale
    'SRS2_SocialAwareness_ScoreRaw': rawScores.socialAwareness,
    'SRS2_SocialCognition_ScoreRaw': rawScores.socialCognition,
    'SRS2_SocialCommunication_ScoreRaw': rawScores.socialCommunication,
    'SRS2_SocialMotivation_ScoreRaw': rawScores.socialMotivation,
    'SRS2_RRB_ScoreRaw': rawScores.rrb,
    'SRS2_SCI_ScoreRaw': rawScores.sci,
    'SRS2_Total_ScoreRaw': rawScores.total,
    
    // T-scores - standardized scores for clinical interpretation
    'SRS2_SocialAwareness_TScore': tScores.socialAwareness,
    'SRS2_SocialCognition_TScore': tScores.socialCognition,
    'SRS2_SocialCommunication_TScore': tScores.socialCommunication,
    'SRS2_SocialMotivation_TScore': tScores.socialMotivation,
    'SRS2_RRB_TScore': tScores.rrb,
    'SRS2_SCI_TScore': tScores.sci,
    'SRS2_Total_TScore': tScores.total,
    
    // Summary interpretation based on total T-score
    'SRS2_ScoreSummary': getSeverityLevel(tScores.total)
  };

  debugTable("All Embedded Data Fields", fieldMappings);

  // Set all embedded data fields
  Object.entries(fieldMappings).forEach(([field, value]) => {
    setEmbeddedData(field, value);
  });

  // Save display order to participant
  const currentOrder = Qualtrics.SurveyEngine.getEmbeddedData("displayOrder");
  let updatedOrder = currentOrder ? currentOrder + ", " + "SRS-2" : "SRS-2";
  Qualtrics.SurveyEngine.setEmbeddedData("displayOrder", updatedOrder);

  debugLog("SRS-2 scoring calculation completed successfully");
  debugLog("=".repeat(50)); // Visual separator for multiple runs
});