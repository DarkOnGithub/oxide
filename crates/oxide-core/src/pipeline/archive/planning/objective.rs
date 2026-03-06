use std::time::Duration;

use crate::CompressionPreset;

#[derive(Debug, Clone, Copy)]
pub struct ObjectiveWeights {
    pub ratio: f64,
    pub speed: f64,
    pub cpu: f64,
    pub memory: f64,
}

impl ObjectiveWeights {
    pub fn for_preset(preset: CompressionPreset) -> Self {
        match preset {
            CompressionPreset::Fast => Self {
                ratio: 0.75,
                speed: 2.0,
                cpu: 1.25,
                memory: 0.2,
            },
            CompressionPreset::Default => Self {
                ratio: 1.75,
                speed: 1.0,
                cpu: 0.75,
                memory: 0.15,
            },
            CompressionPreset::High => Self {
                ratio: 3.0,
                speed: 0.5,
                cpu: 0.4,
                memory: 0.1,
            },
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ObjectiveScore {
    pub ratio_gain: f64,
    pub speed_score: f64,
    pub cpu_cost: f64,
    pub memory_cost: f64,
    pub total: f64,
}

pub fn score_candidate(
    input_bytes: usize,
    stored_bytes: usize,
    elapsed: Duration,
    memory_bytes: usize,
    weights: ObjectiveWeights,
) -> ObjectiveScore {
    let input_bytes = input_bytes.max(1);
    let ratio_gain = (input_bytes as f64 - stored_bytes as f64) / input_bytes as f64;
    let cpu_cost = elapsed.as_secs_f64();
    let speed_score = 1.0 / (1.0 + (cpu_cost * 1000.0));
    let memory_cost = memory_bytes as f64 / 65_536.0;
    let total = (weights.ratio * ratio_gain) + (weights.speed * speed_score)
        - (weights.cpu * cpu_cost)
        - (weights.memory * memory_cost);

    ObjectiveScore {
        ratio_gain,
        speed_score,
        cpu_cost,
        memory_cost,
        total,
    }
}
