use eframe::egui;
use std::fs::File;
use std::path::PathBuf;
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use oxide_core::telemetry::{RunTelemetryOptions, TelemetryEvent, TelemetrySink};
use oxide_core::{ArchivePipeline, ArchivePipelineConfig, BufferPool, CompressionAlgo};

// --- MODÈLES DE DONNÉES ---

#[derive(PartialEq, Clone, Copy)]
enum Mode {
    Compresser,
    Extraire,
    Chiffrer,
    Dechiffrer,
    Proteger, 
    Verifier,
    Reparer,  
}

enum AppMsg {
    Progression(f32),
    Termine(Result<String, String>),
}

struct EguiTelemetrySink {
    tx: Sender<AppMsg>,
    ctx: egui::Context,
}

impl TelemetrySink for EguiTelemetrySink {
    fn on_event(&mut self, event: TelemetryEvent) {
        match event {
            TelemetryEvent::ArchiveProgress(prog) => {
                let ratio = if prog.input_bytes_total > 0 {
                    prog.input_bytes_completed as f32 / prog.input_bytes_total as f32
                } else {
                    0.0
                };
                let _ = self.tx.send(AppMsg::Progression(ratio));
                self.ctx.request_repaint();
            }
            TelemetryEvent::ExtractProgress(prog) => {
                let ratio = if prog.blocks_total > 0 {
                    prog.blocks_completed as f32 / prog.blocks_total as f32
                } else {
                    0.0
                };
                let _ = self.tx.send(AppMsg::Progression(ratio));
                self.ctx.request_repaint();
            }
            _ => {}
        }
    }
}

// --- ÉTAT DE L'APPLICATION ---

pub struct AppCompresseur {
    mode_actuel: Mode,
    fichier_selectionne: Option<PathBuf>,
    en_cours: bool,
    progression: f32,
    message_fin: Option<Result<String, String>>,
    receveur_msg: Option<Receiver<AppMsg>>,
    mot_de_passe: String,
    pourcentage_protection: u8,
    
    // -- NOUVELLES OPTIONS DE COMPRESSION --
    fenetre_options_ouverte: bool,
    config_algo: CompressionAlgo,
    config_block_size_kb: usize,
    config_workers: usize,
}

impl Default for AppCompresseur {
    fn default() -> Self {
        Self {
            mode_actuel: Mode::Compresser,
            fichier_selectionne: None,
            en_cours: false,
            progression: 0.0,
            message_fin: None,
            receveur_msg: None,
            mot_de_passe: String::new(),
            pourcentage_protection: 5,
            
            // Valeurs par défaut d'Oxide Core
            fenetre_options_ouverte: false,
            config_algo: CompressionAlgo::Zstd,
            config_block_size_kb: 1024, // 1MB par défaut
            config_workers: thread::available_parallelism().map(|n| n.get()).unwrap_or(4),
        }
    }
}

impl eframe::App for AppCompresseur {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        // Réception des messages
        if let Some(rx) = &self.receveur_msg {
            while let Ok(msg) = rx.try_recv() {
                match msg {
                    AppMsg::Progression(p) => self.progression = p,
                    AppMsg::Termine(resultat) => {
                        self.en_cours = false;
                        self.progression = 1.0;
                        self.message_fin = Some(resultat);
                    }
                }
            }
        }

        // --- FENÊTRE D'OPTIONS DE COMPRESSION ---
        if self.fenetre_options_ouverte {
            egui::Window::new("Paramètres de Compression")
                .collapsible(false)
                .resizable(false)
                .show(ctx, |ui| {
                    ui.label("Choisissez les réglages d'Oxide Core :");
                    ui.add_space(10.0);

                    // Choix de l'algorithme
                    ui.label("Algorithme :");
                    ui.horizontal(|ui| {
                        ui.selectable_value(&mut self.config_algo, CompressionAlgo::Lz4, "Fast");
                        ui.selectable_value(&mut self.config_algo, CompressionAlgo::Zstd, "Balanced");
                        ui.selectable_value(&mut self.config_algo, CompressionAlgo::Lzma, "Ultra");
                    });

                    ui.add_space(10.0);

                    // Taille des blocs
                    ui.label(format!("Taille des blocs : {} KB", self.config_block_size_kb));
                    ui.add(egui::Slider::new(&mut self.config_block_size_kb, 64..=8192).step_by(64.0));

                    ui.add_space(10.0);

                    // Nombre de workers
                    ui.label("Threads de travail :");
                    ui.add(egui::Slider::new(&mut self.config_workers, 1..=32));

                    ui.add_space(20.0);

                    if ui.button("✅ Valider et Fermer").clicked() {
                        self.fenetre_options_ouverte = false;
                    }
                });
        }

        // --- PANEL PRINCIPAL ---
        egui::CentralPanel::default().show(ctx, |ui| {
            ui.heading("Oxide Toolkit");
            ui.separator();

            // SÉLECTEUR DE MODES
            ui.horizontal(|ui| {
                ui.selectable_value(&mut self.mode_actuel, Mode::Compresser, "📦 Compresser");
                ui.selectable_value(&mut self.mode_actuel, Mode::Extraire, "📂 Extraire");
                ui.selectable_value(&mut self.mode_actuel, Mode::Verifier, "🔍 Vérifier");
                ui.selectable_value(&mut self.mode_actuel, Mode::Proteger, "🛡️ Protéger");
                ui.selectable_value(&mut self.mode_actuel, Mode::Reparer, "🛠️ Réparer");
            });
            ui.separator();

            // SÉLECTION DE FICHIER
            ui.horizontal(|ui| {
                if ui.button("📄 Choisir un fichier").clicked() && !self.en_cours {
                    if let Some(chemin) = rfd::FileDialog::new().pick_file() {
                        self.fichier_selectionne = Some(chemin);
                        self.message_fin = None;
                        self.progression = 0.0;
                    }
                }
                
                if self.mode_actuel == Mode::Compresser {
                    if ui.button("📁 Choisir un dossier").clicked() && !self.en_cours {
                        if let Some(chemin) = rfd::FileDialog::new().pick_folder() {
                            self.fichier_selectionne = Some(chemin);
                            self.message_fin = None;
                            self.progression = 0.0;
                        }
                    }
                    
                    // BOUTON POUR OUVRIR LA FENÊTRE D'OPTIONS
                    if ui.button("⚙️ Options").clicked() {
                        self.fenetre_options_ouverte = true;
                    }
                }
            });

            if let Some(chemin) = &self.fichier_selectionne {
                ui.label(egui::RichText::new(format!("Cible : {}", chemin.display())).italics());
            }

            ui.add_space(15.0);

            // LOGIQUE D'ACTION
            let texte_bouton = match self.mode_actuel {
                Mode::Compresser => "🚀 Lancer la compression",
                Mode::Extraire => "📂 Lancer l'extraction",
                Mode::Proteger => "🛡️ Appliquer la protection",
                Mode::Verifier => "🔍 Vérifier l'intégrité",
                Mode::Reparer => "🛠️ Tenter une réparation",
                _ => "Action"
            };

            if ui.add_enabled(self.fichier_selectionne.is_some() && !self.en_cours, egui::Button::new(texte_bouton)).clicked() {
                self.en_cours = true;
                self.progression = 0.0;
                self.message_fin = None;

                let (tx, rx) = mpsc::channel();
                self.receveur_msg = Some(rx);

                let ctx_clone = ctx.clone();
                let chemin_source = self.fichier_selectionne.clone().unwrap();
                let mode = self.mode_actuel;
                
                // On capture les options configurées
                let algo = self.config_algo;
                let block_size = self.config_block_size_kb * 1024;
                let workers = self.config_workers;

                thread::spawn(move || {
                    let mut sink = EguiTelemetrySink { tx: tx.clone(), ctx: ctx_clone.clone() };
                    
                    let resultat = match mode {
                        Mode::Compresser => {
                            let mut chemin_dest = chemin_source.clone();
                            chemin_dest.set_extension("oxz");

                            let pool = Arc::new(BufferPool::new(128 * 1024 * 1024, 16));
                            let config = ArchivePipelineConfig::new(block_size, workers, pool, algo);
                            let pipeline = ArchivePipeline::new(config);
                            
                            let options = RunTelemetryOptions { progress_interval: Duration::from_millis(50), emit_final_progress: true, include_telemetry_snapshot: false };
                            
                            if let Ok(fichier_sortie) = File::create(&chemin_dest) {
                                if pipeline.archive_path_seekable(&chemin_source, fichier_sortie, options, Some(&mut sink)).is_ok() {
                                    Ok(format!("Succès ({} - {}KB)", match algo { 
                                        CompressionAlgo::Lz4 => "Fast",
                                        CompressionAlgo::Zstd => "Balanced",
                                        CompressionAlgo::Lzma => "Ultra",
                                    }, block_size / 1024))
                                } else { Err("Erreur compression".into()) }
                            } else { Err("Erreur création fichier".into()) }
                        },
                        _ => Ok("Terminé".into())
                    };

                    let _ = tx.send(AppMsg::Termine(resultat));
                    ctx_clone.request_repaint();
                });
            }

            ui.add_space(20.0);

            // Progression et Résultats
            if self.en_cours || (self.progression > 0.0 && self.message_fin.is_none()) {
                ui.add(egui::ProgressBar::new(self.progression).show_percentage().animate(self.en_cours));
            }

            if let Some(res) = &self.message_fin {
                match res {
                    Ok(msg) => ui.colored_label(egui::Color32::GREEN, format!("✅ {}", msg)),
                    Err(err) => ui.colored_label(egui::Color32::RED, format!("❌ {}", err)),
                };
            }
        });
    }
}

fn main() -> eframe::Result<()> {
    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default().with_inner_size([650.0, 400.0]),
        ..Default::default()
    };
    eframe::run_native("Oxide Toolkit", options, Box::new(|_cc| Box::<AppCompresseur>::default()))
}