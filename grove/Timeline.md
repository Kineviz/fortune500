<!--{"pinCode":false,"dname":"6a6c7180-0165-4b0f-b1f8-508b11522f64","codeMode":"javascript2","hide":false}-->
```js
viewof timelineControl = {
  const container = document.createElement("div");
  container.style.margin = "20px 0";
  container.style.padding = "24px";
  container.style.background = "linear-gradient(135deg, #1a1a2e 0%, #16213e 100%)";
  container.style.borderRadius = "12px";
  container.style.border = "2px solid #0f3460";
  container.style.boxShadow = "0 4px 20px rgba(0,0,0,0.3)";
  container.style.fontFamily = "'Segoe UI', Roboto, Helvetica, Arial, sans-serif";
  container.style.color = "#e0e0e0";

  container.innerHTML = `
    <div style="margin-bottom: 20px;">
      <h2 style="margin: 0 0 8px 0; font-size: 1.5em; color: #e94560; display: flex; align-items: center; gap: 10px;">
        📅 Timeline Visualizer
      </h2>
      <p style="margin: 0; color: #a0a0a0; font-size: 0.95em;">
        Plot nodes on a temporal axis. Ensure your nodes have a date property in ISO format.
      </p>
    </div>

    <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 20px; margin-bottom: 20px;">
      <div>
        <label style="display: block; margin-bottom: 8px; color: #1a936f; font-weight: 600; font-size: 0.9em; text-transform: uppercase; letter-spacing: 1px;">
          Date Field Name
        </label>
        <input type="text" id="date-field-input" value="year" style="
          width: 100%;
          padding: 10px 12px;
          background: #0d1b2a;
          border: 2px solid #0f3460;
          border-radius: 8px;
          color: #e0e0e0;
          font-size: 1em;
          outline: none;
          transition: border-color 0.2s;
        " onfocus="this.style.borderColor='#e94560'" onblur="this.style.borderColor='#0f3460'">
      </div>
      <div>
        <label style="display: block; margin-bottom: 8px; color: #1a936f; font-weight: 600; font-size: 0.9em; text-transform: uppercase; letter-spacing: 1px;">
          Orientation
        </label>
        <div style="display: flex; gap: 15px; padding: 8px 0;">
          <label style="display: flex; align-items: center; gap: 8px; cursor: pointer;">
            <input type="radio" name="orientation" value="down" checked style="accent-color: #e94560;"> Down
          </label>
          <label style="display: flex; align-items: center; gap: 8px; cursor: pointer;">
            <input type="radio" name="orientation" value="up" style="accent-color: #e94560;"> Up
          </label>
        </div>
      </div>
    </div>

    <button id="plot-timeline-btn" style="
      width: 100%;
      background: linear-gradient(135deg, #e94560 0%, #c73659 100%);
      color: white;
      border: none;
      padding: 14px;
      border-radius: 8px;
      cursor: pointer;
      font-size: 1.1em;
      font-weight: bold;
      transition: all 0.2s;
      box-shadow: 0 4px 15px rgba(233, 69, 96, 0.3);
      display: flex;
      align-items: center;
      justify-content: center;
      gap: 10px;
      margin-bottom: 15px;
    " onmouseover="this.style.transform='translateY(-2px)'; this.style.boxShadow='0 6px 20px rgba(233, 69, 96, 0.4)';" 
      onmouseout="this.style.transform='translateY(0)'; this.style.boxShadow='0 4px 15px rgba(233, 69, 96, 0.3)';">
      🚀 Generate Timeline View
    </button>

    <div style="display: flex; gap: 10px;">
      <button id="link-risks-btn" style="
        flex: 1;
        background: rgba(26, 147, 111, 0.1);
        color: #1a936f;
        border: 1px solid #1a936f;
        padding: 10px;
        border-radius: 8px;
        cursor: pointer;
        font-weight: 600;
        transition: all 0.2s;
      " onmouseover="this.style.background='rgba(26, 147, 111, 0.2)'" onmouseout="this.style.background='rgba(26, 147, 111, 0.1)'">
        🔗 Link Categories
      </button>
      <button id="clear-links-btn" style="
        flex: 1;
        background: rgba(233, 69, 96, 0.1);
        color: #e94560;
        border: 1px solid #e94560;
        padding: 10px;
        border-radius: 8px;
        cursor: pointer;
        font-weight: 600;
        transition: all 0.2s;
      " onmouseover="this.style.background='rgba(233, 69, 96, 0.2)'" onmouseout="this.style.background='rgba(233, 69, 96, 0.1)'">
        🗑️ Clear Links
      </button>
    </div>
  `;

  const btn = container.querySelector("#plot-timeline-btn");
  btn.onclick = () => {
    const field = container.querySelector("#date-field-input").value;
    const orient = container.querySelector('input[name="orientation"]:checked').value;
    mutable datefield = field;
    mutable orientation = orient;
    plotMsgTimeline();
  };

  const linkBtn = container.querySelector("#link-risks-btn");
  linkBtn.onclick = () => {
    linkTimelineRisks();
    gxr.toast().success("Risks linked by category");
  };

  const clearBtn = container.querySelector("#clear-links-btn");
  clearBtn.onclick = () => {
    gxr.edges({relationship: 'EVOLVES_TO'}).remove();
    gxr.dispatchGraphDataUpdate();
    gxr.toast().info("Timeline links cleared");
  };

  return container;
}
```

<!--{"pinCode":false,"dname":"27dd0dc5-2826-4590-a6df-9280a54ceea2","codeMode":"javascript2","hide":true}-->
```js
mutable datefield = "year"
```

<!--{"pinCode":true,"dname":"de99a210-181a-4f6e-aa31-9040fdb3727e","codeMode":"javascript2","hide":true}-->
```js
mutable orientation = "down"
```

<!--{"pinCode":false,"dname":"84980f46-8f79-407a-bd58-e70dc8217b7b","codeMode":"javascript2","hide":true}-->
```js
CANONICAL_RISKS = ({
  MACRO_GEO: "Macroeconomic & Geopolitical Conditions",
  COMPETITION : "Competition & Market Dynamics",
  MONETIZATION: "Revenue Model & Monetization Risk",
  USER_GROWTH: "User Growth, Engagement & Demand",
  TECH_EXEC: "Technology Execution & Innovation Risk",
  AI: "Artificial Intelligence Risk",
  DATA_PRIVACY: "Data Privacy, Security & Data Transfer",
  REGULATORY: "Regulatory, Antitrust & Government Scrutiny",
  TAX: "Taxation & Fiscal Uncertainty",
  LEGAL_IP: "Legal Proceedings & Intellectual Property",
  SUPPLY_CHAIN: "Supply Chain, Manufacturing & Operations",
  TALENT: "Talent, Labor & Organizational Capability",
  PLATFORM: "Platform, Partner & Third-Party Dependency",
  ESG: "ESG, Climate & Sustainability",
  FINANCIAL: "Financial Performance, Margins & Capital Risk"
})
```

<!--{"pinCode":false,"dname":"93893107-5878-47cd-bd04-f0bc98c0c5cf","codeMode":"javascript2","hide":true}-->
```js
RULES = ({
  MACRO_GEO: ["macroeconomic", "geopolitical", "pandemic", "covid", "inflation", "trade dispute", "global uncertainty"],
  COMPETITION: ["competition", "competitor", "price competition", "new entrant", "market pressure"],
  MONETIZATION: ["advertising revenue", "ad prices", "monetization", "revenue mix", "pricing pressure"],
  USER_GROWTH: ["user growth", "engagement", "declining users", "slowing growth", "adoption"],
  TECH_EXEC: ["technological irrelevance", "innovation", "erp", "execution risk", "platform evolution"],
  AI: ["artificial intelligence", "ai ", "machine learning", "model risk", "ai liability"],
  DATA_PRIVACY: ["data privacy", "cybersecurity", "data breach", "data transfer", "gdpr", "ccpa", "ad targeting"],
  REGULATORY: ["regulatory", "antitrust", "government scrutiny", "ftc", "dma", "dsa", "section 230"],
  TAX: ["tax", "irs", "transfer pricing", "digital tax", "tax liability"],
  LEGAL_IP: ["litigation", "lawsuit", "intellectual property", "patent", "legal proceeding"],
  SUPPLY_CHAIN: ["supply chain", "manufacturing", "component", "logistics", "inventory"],
  TALENT: ["talent", "labor", "personnel", "workforce", "retention"],
  PLATFORM: ["third-party", "platform dependency", "developers", "app store", "partner"],
  ESG: ["esg", "climate", "sustainability", "environmental"],
  FINANCIAL: ["margin pressure", "profitability", "operating loss", "credit risk", "financial loss"]
})
```

<!--{"pinCode":false,"dname":"32835a37-b227-48b3-b6e8-fb6bf17c80ed","codeMode":"javascript2","hide":true}-->
```js
function mapRisk(riskText) {
  const text = riskText.toLowerCase().replace(/[^a-z0-9\s]/g, " ").replace(/\s+/g, " ");
  let best = { key: "UNMAPPED", score: 0 };
  for (const [key, terms] of Object.entries(RULES)) {
    const score = terms.filter(t => text.includes(t)).length;
    if (score > best.score) best = { key, score };
  }
  return {
    original: riskText,
    canonicalKey: best.key,
    canonicalLabel: CANONICAL_RISKS[best.key] || "Other / Review",
    confidence: best.score === 0 ? 0.25 : Math.min(0.95, 0.4 + best.score * 0.15)
  };
}
```

<!--{"pinCode":false,"dname":"f13bec6f-1855-4457-b9af-e068b500b561","codeMode":"javascript2","hide":true}-->
```js
performTimelineMapping = () => {
  gxr.nodes({category: 'Risk'}).forEach(n => {
    // Try to find the canonical key if it's missing
    if (!n.properties.key) {
      // 1. Try exact match from existing label
      if (n.properties.label) {
        const exactKey = _.findKey(CANONICAL_RISKS, v => v === n.properties.label);
        if (exactKey) n.properties.key = exactKey;
      }
      
      // 2. Use heuristic mapping if still no key
      if (!n.properties.key) {
        const risk = mapRisk(n.properties.label || n.properties.id || n.id);
        n.properties.key = risk.canonicalKey;
      }
    }
    
    // Ensure we have a label for display, but don't overwrite existing ones
    if (!n.properties.label && n.properties.key) {
      n.properties.label = CANONICAL_RISKS[n.properties.key] || "Other / Review";
    }
  });
}
```

<!--{"pinCode":false,"dname":"801e05a2-066c-47b0-b8b5-e1ce47204a40","codeMode":"javascript2","hide":true}-->
```js
linkTimelineRisks = () => {
  const risks = gxr.nodes({category: 'Risk'}).array;
  const risksByKey = _.groupBy(risks, n => n.properties.key || 'unmapped');
  const edges = [];
  
  gxr.edges({relationship: 'EVOLVES_TO'}).remove();
  
  for (const key in risksByKey) {
    if (key === 'unmapped' || key === 'UNMAPPED') continue;
    const byYear = _.groupBy(risksByKey[key], n => n.properties[datefield]);
    const years = _.keys(byYear).sort((a, b) => parseFloat(a) - parseFloat(b));
    
    for (let i = 0; i < years.length - 1; i++) {
      const sources = byYear[years[i]];
      const targets = byYear[years[i+1]];
      sources.forEach(s => {
        let t = targets.find(target => target.properties.id === s.properties.id);
        if (!t && targets.length > 0) t = targets[0];
        if (t) edges.push({ sourceId: s.id, targetId: t.id, relationship: 'EVOLVES_TO' });
      });
    }
  }
  if (edges.length > 0) {
    gxr.addEdges(edges);
    gxr.edges({relationship: 'EVOLVES_TO'}).style('opacity', 0.4);
  }
}
```

<!--{"pinCode":false,"dname":"2e3c10a3-c496-4ec3-b619-6aa731328b51","codeMode":"javascript2","hide":true}-->
```js
plotMsgTimeline = async ()=>{ 
  let max_y= 0.7
  let min_y=-0.7
  let y_spread = max_y - min_y
  let x_spread = 2*y_spread

  // 1. Mapping
  performTimelineMapping();

  await createDateNodes()
  
//   use __gxr_date__ field so we can plot timeline in both directions
  gxr.nodes()
    .filter(n=>n.properties[datefield])
    .forEach(n=>n.properties.__gxr_date__ = Date.parse(n.properties[datefield]) * (orientation == 'up' ? -1 : 1))
  
  await gxr.sleep(300)
  gxr.nodes()
    .forEach(n=>{
      n.position.x = 0
      n.position.y = 0
  })
  await gxr.sleep(300)

  gxr.nodes().distributionBy({
    dimension: 'x',
    bin: datefield,
    binType: 'date',
    spread: x_spread,
  })

  await gxr.sleep(200)
  
  // 3. Vertical Spread within each year
  const risks = gxr.nodes({category: 'Risk'}).array;
  const risksByYear = _.groupBy(risks, n => n.properties[datefield]);
  
  for (const year in risksByYear) {
    const yearRisks = risksByYear[year];
    if (yearRisks.length === 0) continue;
    
    // Calculate even spacing between max_y and min_y
    // Leave a small margin so risks aren't directly on top of the Date nodes
    const margin = 0.1;
    const availableSpace = (max_y - margin) - (min_y + margin);
    const step = yearRisks.length > 1 ? availableSpace / (yearRisks.length - 1) : 0;
    
    // Sort risks by ID or some property for deterministic order if desired
    const sortedRisks = _.sortBy(yearRisks, n => n.properties.id || n.id);
    
    sortedRisks.forEach((n, i) => {
      n.position.y = (max_y - margin) - (i * step);
    });
  }
  
  await gxr.sleep(200)

  // 4. Pin Date nodes to the absolute extremes
  gxr.nodes({category:'Date'})
    .filter(n=>n.properties.location==1)
    .forEach(n=>n.position.y = max_y)
  
  gxr.nodes({category:'Date'})
    .filter(n=>n.properties.location==-1)
    .forEach(n=>n.position.y = min_y)  
  
  gxr.nodes({category:'Game'})
    .forEach(n=>n.position.y = max_y + 0.15)  
  
  await gxr.sleep(300)
  gxr.nodes()
    .filter(n=>n.properties.__gxr_date__)
    .forEach(n=>{delete n.properties.__gxr_date__})

  // 2. Linking
  linkTimelineRisks();
  
  gxr.dispatchGraphDataUpdate();
}
```

<!--{"pinCode":false,"dname":"cffa6d29-0dc0-4e28-ad9f-69e5e32fd25a","codeMode":"javascript2","hide":true}-->
```js
getDateStrings = (raw_dates, max_ticks = 10, opts = {}) => {
  if (!raw_dates || !raw_dates.length) return [];

  const { mode = "auto" } = opts; // "auto" | "year"

  // --- helpers -------------------------------------------------
  const normalizeIso = s =>
    typeof s === "string" ? s.replace(/(\.\d{3})\d+$/, "$1") : s;

  const isYear = v =>
    (typeof v === "number" && Number.isInteger(v) && v >= 1000 && v <= 3000) ||
    (typeof v === "string" &&
      /^\s*\d{4}\s*$/.test(v) &&
      +v.trim() >= 1000 &&
      +v.trim() <= 3000);

  const toDate = v => {
    if (v instanceof Date) return v;

    // Year input: Jan 1 of that year in UTC (avoids 1969/1970 timezone edge cases)
    if (isYear(v)) {
      const y = +String(v).trim();
      return new Date(Date.UTC(y, 0, 1));
    }

    // ISO strings (trim + microseconds normalization)
    const s = typeof v === "string" ? v.trim() : v;
    return new Date(normalizeIso(s));
  };
  // ------------------------------------------------------------

  const parsed = raw_dates.map(toDate).filter(d => !isNaN(d));
  if (!parsed.length) return [];

  const min = new Date(Math.min(...parsed.map(d => +d)));
  const max = new Date(Math.max(...parsed.map(d => +d)));

  // formatters (local)
  const pad = n => String(n).padStart(2, "0");
  const formatDate = d =>
    `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}`;
  const formatDateTime = d =>
    `${formatDate(d)}T${pad(d.getHours())}:${pad(d.getMinutes())}`;

  // yearly formatter (UTC)
  const formatYear = d => String(d.getUTCFullYear());

  const yearOnlyInput = raw_dates.every(isYear);
  const useYearly = mode === "year" || (mode === "auto" && yearOnlyInput);

  // single value
  if (+min === +max) {
    return [useYearly ? String(min.getUTCFullYear()) : formatDateTime(min)];
  }

  let interval, totalUnits, formatter;

  if (useYearly) {
    // ✅ UTC-aligned year ticks to match Date.UTC(...) year parsing
    interval = d3.utcYear;
    totalUnits = Math.max(1, max.getUTCFullYear() - min.getUTCFullYear());
    formatter = formatYear;
  } else {
    const diffMs = max - min;
    const diffH = diffMs / 36e5;
    const diffD = diffH / 24;

    if (diffD >= 3) {
      interval = d3.timeDay;
      totalUnits = Math.max(1, Math.round(diffMs / 86400000));
      formatter = formatDate;
    } else if (diffH >= 3) {
      interval = d3.timeHour;
      totalUnits = Math.max(1, Math.round(diffMs / 36e5));
      formatter = formatDateTime;
    } else {
      interval = d3.timeMinute;
      totalUnits = Math.max(1, Math.round(diffMs / 60000));
      formatter = formatDateTime;
    }
  }

  const step = Math.max(1, Math.ceil(totalUnits / max_ticks));
  const it = interval.every(step);

  let ticks = it.range(interval.floor(min), max);

  if (!ticks.length) {
    ticks = [interval.floor(min), interval.floor(max)];
  } else {
    const last = interval.floor(max);
    if (+ticks[ticks.length - 1] !== +last) ticks.push(last);
  }

  if (ticks.length > max_ticks) {
    const stride = Math.ceil(ticks.length / max_ticks);
    ticks = ticks.filter((_, i) => i % stride === 0);
    ticks[ticks.length - 1] = interval.floor(max);
  }

  return ticks.map(formatter);
};
```

<!--{"pinCode":false,"dname":"e30e4861-90dd-46f2-a976-c813ba05366d","codeMode":"javascript2","hide":true}-->
```js
getDateStringsOld = (raw_dates, max_ticks = 10) => {
  if (!raw_dates || !raw_dates.length) return [];

  // Accept Date or ISO string with microseconds
  const normalizeIso = s => typeof s === "string"
    ? s.replace(/(\.\d{3})\d+$/, "$1")
    : s;
  const toDate = v => v instanceof Date ? v : new Date(normalizeIso(v));
  const parsed = raw_dates.map(toDate).filter(d => !isNaN(d));
  if (!parsed.length) return [];

  const min = new Date(Math.min(...parsed.map(d => +d)));
  const max = new Date(Math.max(...parsed.map(d => +d)));
  if (+min === +max) return [formatDateTime(min)]; // single point

  const diffMs = max - min;
  const diffH = diffMs / 36e5;
  const diffD = diffH / 24;

  // Formatters
  function pad(n){ return String(n).padStart(2,"0"); }
  function formatDate(d){
    return `${d.getFullYear()}-${pad(d.getMonth()+1)}-${pad(d.getDate())}`;
  }
  function formatDateTime(d){
    return `${formatDate(d)}T${pad(d.getHours())}:${pad(d.getMinutes())}`;
  }

  // Choose interval & formatter
  let interval, totalUnits, useDateOnly;
  if (diffD >= 3) {
    // Many days: date-only labels, day-based ticks
    useDateOnly = true;
    interval = d3.timeDay;
    totalUnits = Math.max(1, Math.round(diffMs / 86400000)); // days
  } else if (diffH >= 3) {
    // < 3 days: hour ticks, show time
    useDateOnly = false;
    interval = d3.timeHour;
    totalUnits = Math.max(1, Math.round(diffMs / 36e5)); // hours
  } else {
    // < 3 hours: minute ticks, show time
    useDateOnly = false;
    interval = d3.timeMinute;
    totalUnits = Math.max(1, Math.round(diffMs / 60000)); // minutes
  }

  // Pick a step so we aim for <= max_ticks
  const step = Math.max(1, Math.ceil(totalUnits / max_ticks));
  const it = interval.every(step);

  // Build ticks aligned to interval boundaries
  let ticks = it.range(interval.floor(min), max);

  // Ensure we have at least one tick and include the end if aligned
  if (!ticks.length) ticks = [interval.floor(min), interval.floor(max)];
  else {
    const last = interval.floor(max);
    if (+ticks[ticks.length - 1] !== +last && +last > +ticks[ticks.length - 1]) {
      ticks.push(last);
    }
  }

  // Hard cap to max_ticks (downsample)
  if (ticks.length > max_ticks) {
    const stride = Math.ceil(ticks.length / max_ticks);
    ticks = ticks.filter((_, i) => i % stride === 0);
    // make sure last tick aligns with end
    const last = interval.floor(max);
    if (+ticks[ticks.length - 1] !== +last) {
      ticks[ticks.length - 1] = last;
    }
  }

  return ticks.map(useDateOnly ? formatDate : formatDateTime);
};
```

<!--{"pinCode":false,"dname":"18f32932-5608-44f6-8157-c67814c7d85a","codeMode":"javascript2","hide":true}-->
```js
createDateNodes = async ()=>
{
//   remove existing Date nodes()
  gxr.nodes({category:'Date'}).remove()
  await gxr.sleep(500)
  
  let raw_dates = gxr.nodes()
    .filter(n=>n.properties[datefield])
    .map(n=>n.properties[datefield])

  const dateStrings = getDateStrings(raw_dates)
//   return dateStrings

  let nodes_data_up= dateStrings.map(d=>{
    let prop = {}; prop[datefield]=d;
    return {id: d+'-up', category:'Date', properties:{...prop, location: 1}}
  })
  let nodes_data_down= dateStrings.map(d=>{
    let prop = {}; prop[datefield]=d;
    return {id: d+'-down', category:'Date', properties:{...prop, location: -1}}
  })

  let nodes_data = _.flatten([nodes_data_up, nodes_data_down])
  
  gxr.add(nodes_data)
  
  let edge_list = dateStrings.map(d=>{
    return {sourceId: d+'-down', targetId: d+'-up'}
  })
  gxr.add(edge_list)
  gxr.dispatchGraphDataUpdate();
  return edge_list
  
}
```

<!--{"pinCode":false,"dname":"b181e914-d82e-4777-8a7b-ac6d65fa1cdf","codeMode":"javascript2","hide":true}-->
```js
md `
---
### 🛠️ Configuration & Helpers
<p style="color: #a0a0a0; font-size: 0.85em;">
  This section contains the underlying logic for risk mapping, coordinate calculations, and graph transformations.
</p>
`
```

<!--{"pinCode":false,"dname":"43c6efac-6f9b-436c-84eb-72f65cb14761","codeMode":"javascript2","hide":true}-->
```js
API=_app.controller.API
```

<!--{"pinCode":false,"dname":"2482842d-c4db-4a55-963b-e62ba3f8b10d","codeMode":"javascript2","hide":true}-->
```js
function debounce(input, delay = 1000) {
  return Generators.observe(notify => {
    let timer = null;
    let value;

    // On input, check if we recently reported a value.
    // If we did, do nothing and wait for a delay;
    // otherwise, report the current value and set a timeout.
    function inputted() {
      if (timer !== null) return;
      notify(value = input.value);
      timer = setTimeout(delayed, delay);
    }

    // After a delay, check if the last-reported value is the current value.
    // If it’s not, report the new value.
    function delayed() {
      timer = null;
      if (value === input.value) return;
      notify(value = input.value);
    }

    input.addEventListener("input", inputted), inputted();
    return () => input.removeEventListener("input", inputted);
  });
}
```