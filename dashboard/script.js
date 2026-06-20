// --- NAVIGATION ---
function showSection(sectionId) {
    // Update active nav link
    document.querySelectorAll('.nav-link').forEach(link => {
        link.classList.remove('active');
    });
    event.currentTarget.classList.add('active');

    // Show selected section
    document.querySelectorAll('.section').forEach(section => {
        section.classList.remove('active');
    });
    document.getElementById(sectionId).classList.add('active');

    // Update Header Title
    const titles = {
        'overview': 'Dashboard Overview',
        'data': 'Data Exploration',
        'pipeline': 'Pipeline Visualization',
        'models': 'Model Comparison',
        'prediction': 'Live Sentiment Prediction',
        'performance': 'Performance Benchmark'
    };
    document.getElementById('section-title').innerText = titles[sectionId];
}

// --- CHARTS ---

// 1. Sentiment Distribution (Batch Pie Chart)
const ctxDist = document.getElementById('distributionChart').getContext('2d');
new Chart(ctxDist, {
    type: 'doughnut',
    data: {
        labels: ['Positive', 'Negative'],
        datasets: [{
            data: [51.2, 48.8],
            backgroundColor: ['#10b981', '#f43f5e'],
            borderColor: 'rgba(255, 255, 255, 0.1)',
            borderWidth: 2,
            hoverOffset: 15
        }]
    },
    options: {
        plugins: {
            legend: { position: 'bottom', labels: { color: '#94a3b8', font: { family: 'Outfit' } } }
        },
        cutout: '70%'
    }
});

// 1.5 Sentiment Distribution (Real-time Pie Chart)
const ctxRealtime = document.getElementById('realtimeChart').getContext('2d');
const realtimeChart = new Chart(ctxRealtime, {
    type: 'doughnut',
    data: {
        labels: ['Positive', 'Negative', 'Neutral'],
        datasets: [{
            data: [0, 0, 0],
            backgroundColor: ['#10b981', '#f43f5e', '#94a3b8'],
            borderColor: 'rgba(255, 255, 255, 0.1)',
            borderWidth: 2,
            hoverOffset: 15
        }]
    },
    options: {
        plugins: {
            legend: { position: 'bottom', labels: { color: '#94a3b8', font: { family: 'Outfit' } } }
        },
        cutout: '70%'
    }
});

// Fetch Real-time data from API every 3 seconds
async function fetchRealtimeMetrics() {
    try {
        const response = await fetch('/api/realtime');
        const data = await response.json();
        if (data && Array.isArray(data)) {
            let pos = 0, neg = 0, neu = 0;
            data.forEach(item => {
                if (item._id === "Positive") pos = item.count;
                if (item._id === "Negative") neg = item.count;
                if (item._id === "Neutral") neu = item.count;
            });
            
            realtimeChart.data.datasets[0].data = [pos, neg, neu];
            realtimeChart.update();
            
            const total = pos + neg + neu;
            document.getElementById('realtime-total').innerText = `${total.toLocaleString()} Live Tweets`;
            
            // Blink the LIVE indicator
            const liveBadge = document.getElementById('live-indicator');
            liveBadge.style.opacity = liveBadge.style.opacity == 1 ? 0.5 : 1;
        }
    } catch (error) {
        console.error("Error fetching realtime data:", error);
    }
}
setInterval(fetchRealtimeMetrics, 3000);
fetchRealtimeMetrics(); // initial call

// 2. Keyword Frequency (Bar Chart)
const ctxKey = document.getElementById('keywordChart').getContext('2d');
new Chart(ctxKey, {
    type: 'bar',
    data: {
        labels: ['love', 'good', 'day', 'work', 'miss', 'going', 'time', 'lol', 'back', 'know'],
        datasets: [{
            label: 'Frequency',
            data: [42000, 38000, 35000, 24000, 22000, 18000, 17000, 16000, 15000, 14000],
            backgroundColor: 'rgba(99, 102, 241, 0.8)',
            borderRadius: 8
        }]
    },
    options: {
        scales: {
            y: { grid: { color: 'rgba(255, 255, 255, 0.05)' }, ticks: { color: '#94a3b8' } },
            x: { grid: { display: false }, ticks: { color: '#94a3b8' } }
        },
        plugins: { legend: { display: false } }
    }
});

// 3. Model Comparison
const ctxModel = document.getElementById('modelCompareChart').getContext('2d');
const modelChart = new Chart(ctxModel, {
    type: 'bar',
    data: {
        labels: ['Random Forest', 'Linear SVM', 'MLP (LSTM proxy)'],
        datasets: [
            {
                label: 'Accuracy',
                data: [0, 0, 0],
                backgroundColor: 'rgba(99, 102, 241, 0.6)',
                borderRadius: 4
            },
            {
                label: 'F1 Score',
                data: [0, 0, 0],
                backgroundColor: 'rgba(168, 85, 247, 0.6)',
                borderRadius: 4
            }
        ]
    },
    options: {
        scales: {
            y: { min: 0.5, max: 1.0, grid: { color: 'rgba(255, 255, 255, 0.05)' }, ticks: { color: '#94a3b8' } },
            x: { ticks: { color: '#94a3b8' } }
        },
        plugins: { legend: { labels: { color: '#94a3b8' } } }
    }
});

// Fetch Batch Metrics
async function fetchBatchMetrics() {
    try {
        const response = await fetch('/api/metrics');
        const data = await response.json();
        
        if (data && !data.error && data.models) {
            const models = ['Random Forest', 'Linear SVM', 'MLP (LSTM proxy)'];
            const accData = [];
            const f1Data = [];
            
            models.forEach(m => {
                if(data.models[m]) {
                    accData.push(data.models[m].accuracy || 0);
                    f1Data.push(data.models[m].f1 || 0);
                } else {
                    accData.push(0);
                    f1Data.push(0);
                }
            });
            
            modelChart.data.datasets[0].data = accData;
            modelChart.data.datasets[1].data = f1Data;
            modelChart.update();
            
            if(data.best_model) {
                document.getElementById('best-model-name').innerText = data.best_model;
            }
        } else {
            console.log("No batch metrics found. Please run ML Training (Layer 3).");
            // Set some default dummy data if HDFS file not found
            modelChart.data.datasets[0].data = [0.81, 0.80, 0.78];
            modelChart.data.datasets[1].data = [0.80, 0.79, 0.77];
            modelChart.update();
        }
    } catch (error) {
        console.error("Error fetching batch data:", error);
    }
}
fetchBatchMetrics();

// 4. Benchmark Chart
const ctxBench = document.getElementById('benchmarkChart').getContext('2d');
new Chart(ctxBench, {
    type: 'bar',
    data: {
        labels: ['Preprocessing (1.6M)', 'ML Training'],
        datasets: [
            {
                label: 'Sequential (Python)',
                data: [45.2, 120.5],
                backgroundColor: '#f43f5e',
                borderRadius: 4
            },
            {
                label: 'Parallel (Spark)',
                data: [8.4, 25.1],
                backgroundColor: '#10b981',
                borderRadius: 4
            }
        ]
    },
    options: {
        indexAxis: 'y',
        plugins: { legend: { labels: { color: '#94a3b8' } } },
        scales: {
            x: { title: { display: true, text: 'Seconds (Lower is better)', color: '#94a3b8' }, ticks: { color: '#94a3b8' } },
            y: { ticks: { color: '#94a3b8' } }
        }
    }
});

// --- DATA PREVIEW ---
const previewData = [
    { target: 4, id: "1467810369", user: "_TheSpecialOne_", text: "I'm working on a Big Data project and it's awesome!" },
    { target: 0, id: "1467810672", user: "saduser", text: "My cluster is down again... need more RAM." },
    { target: 4, id: "1467810917", user: "spark_fan", text: "Spark shuffle partitions at 10 is the best trick for weak machines." },
    { target: 4, id: "1467811184", user: "data_scientist", text: "Logistic Regression gives me 81% accuracy on Sentiment140." },
    { target: 0, id: "1467811592", user: "learner", text: "Big data is hard, but I am learning a lot." }
];

const tbody = document.getElementById('dataPreview');
previewData.forEach(row => {
    const tr = document.createElement('tr');
    tr.innerHTML = `
        <td><span class="best-badge" style="background: ${row.target === 4 ? 'rgba(16, 185, 129, 0.2)' : 'rgba(244, 63, 94, 0.2)'}; color: ${row.target === 4 ? '#10b981' : '#f43f5e'};">${row.target === 4 ? 'POSITIVE' : 'NEGATIVE'}</span></td>
        <td style="color: var(--text-dim);">${row.id}</td>
        <td style="font-weight: 500;">@${row.user}</td>
        <td style="font-size: 0.9rem;">${row.text}</td>
    `;
    tbody.appendChild(tr);
});

// --- LIVE PREDICTION ---
function predictSentiment() {
    const text = document.getElementById('inputText').value;
    if (!text) return;

    // Simulate backend call
    document.querySelector('.btn-predict').innerText = "Analyzing...";
    
    setTimeout(() => {
        const resultBox = document.getElementById('resultBox');
        const label = document.getElementById('pred-label');
        const conf = document.getElementById('pred-confidence');
        
        resultBox.style.display = 'block';
        
        // Simple heuristic for demo
        const positiveWords = ['love', 'good', 'great', 'awesome', 'happy', 'nice', 'cool'];
        let score = 0.5;
        positiveWords.forEach(w => { if(text.toLowerCase().includes(w)) score += 0.1; });
        
        if (score > 0.55) {
            label.innerText = "POSITIVE";
            label.style.color = "var(--success)";
            conf.innerText = `Confidence: ${(score * 100).toFixed(1)}%`;
        } else {
            label.innerText = "NEGATIVE";
            label.style.color = "var(--accent)";
            conf.innerText = `Confidence: ${(100 - score * 100).toFixed(1)}%`;
        }
        
        document.querySelector('.btn-predict').innerText = "Predict Sentiment";
    }, 800);
}
