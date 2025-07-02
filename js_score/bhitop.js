Qualtrics.SurveyEngine.addOnPageSubmit(function() {
  // MUST ADD "B-HiTOP_Internalizing", "B-HiTOP_Somatoform", "B-HiTOP_Detachment", 
  // "B-HiTOP_ThghtDis", "B-HiTOP_Disinhibition", & "B-HiTOP_Antagonism" 
  // AS EMBEDDED DATA BEFORE THIS BLOCK
  
  const DEBUG_TABLES = false;

  // Scale configuration - defines items and scoring rules for each scale
  const SCALE_CONFIG = {
    Internalizing: {
      items: ["HiTOP_69", "HiTOP_187", "HiTOP_378", "HiTOP_570",
              "HiTOP_333", "HiTOP_356", "HiTOP_368", "HiTOP_215"],
      topK: 7
    },
    Somatoform: {
      items: ["HiTOP_479", "HiTOP_449", "HiTOP_451", "HiTOP_490",
              "HiTOP_494", "HiTOP_456", "HiTOP_487", "HiTOP_492"],
      topK: 7
    },
    Detachment: {
      items: ["HiTOP_50", "HiTOP_624", "HiTOP_44", "HiTOP_625", "HiTOP_657"],
      topK: 4
    },
    ThghtDis: {
      items: ["HiTOP_606", "HiTOP_596", "HiTOP_554", "HiTOP_558",
              "HiTOP_583", "HiTOP_557"],
      topK: 5
    },
    Disinhibition: {
      items: ["Ext_102", "Ext_320", "Ext_256", "Ext_361", "Ext_374",
              "Ext_166", "Ext_281", "Ext_97", "Ext_13"],
      topK: 7
    },
    Antagonism: {
      items: ["Ext_262", "Ext_22", "Ext_370", "Ext_175", "HiTOP_577",
              "HiTOP_11", "Ext_367", "HiTOP_21", "Ext_50"],
      topK: 7
    }
  };

  // ── Helper Functions ──────────────────────────────────
  function collectResponses(container) {
    const rows = container.querySelectorAll('.ChoiceRow');
    const responses = new Map();
    const itemDetails = [];
    let validRowCount = 0;
    
    rows.forEach((row, index) => {
      const span = row.querySelector('span[data-row-id]');
      if (!span) return;
      
      validRowCount++;
      
      const itemId = span.getAttribute('data-row-id');
      const input = row.querySelector('input[type="radio"]:checked');
      const textCell = row.querySelector('label');
      const value = input ? parseInt(input.value, 10) : null;
      
      if (value !== null) {
        responses.set(itemId, value);
      }
      
      let itemText = 'N/A';
      if (textCell) {
        itemText = textCell.textContent.trim();
      }
      
      itemDetails.push({
        Index: index,
        Row: validRowCount,
        ItemID: itemId,
        Text: itemText,
        Status: value !== null ? 'Answered' : 'Unanswered',
        ResponseNum: value || '',

      });
    });
    
    return { responses, itemDetails };
  }

  function calculateScaleScores(config, responses) {
    const results = [];
    
    for (const [scaleName, { items, topK }] of Object.entries(config)) {
      const availableItems = items
        .map(itemId => ({ itemId, value: responses.get(itemId) }))
        .filter(entry => entry.value !== undefined);
      
      const fieldName = 'B-HiTOP_' + scaleName;
      
      if (availableItems.length === 0) {
        Qualtrics.SurveyEngine.setEmbeddedData(fieldName, 0);
        results.push({
          Scale: scaleName,
          Score: 0,
          ItemsUsed: '0/0',
          EmbeddedField: fieldName
        });
        continue;
      }
      
      availableItems.sort((a, b) => b.value - a.value);
      const topItems = availableItems.slice(0, Math.min(topK, availableItems.length));
      
      const score = +(topItems.reduce((sum, item) => sum + item.value, 0) / topItems.length).toFixed(2);
      
      Qualtrics.SurveyEngine.setEmbeddedData(fieldName, score);
      
      results.push({
        Scale: scaleName,
        Score: score,
        ItemsUsed: topItems.length + '/' + availableItems.length,
        EmbeddedField: fieldName
      });
    }
    
    return results;
  }

  // ── Main Execution ────────────────────────────────────
  const container = this.getQuestionContainer();
  const { responses, itemDetails } = collectResponses(container);
  
  if (DEBUG_TABLES && itemDetails.length > 0) {
    const answeredCount = itemDetails.filter(item => item.Value !== '').length;
    console.group('Item Responses Summary');
    console.table(itemDetails);
    console.groupEnd();
  }
  
  const results = calculateScaleScores(SCALE_CONFIG, responses);
  
  if (DEBUG_TABLES && results.length > 0) {
    console.group('Scale Scores Summary');
    console.table(results);
    console.groupEnd();
  }
});