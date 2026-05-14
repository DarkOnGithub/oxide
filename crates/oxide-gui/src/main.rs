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
    pourcentage_protection: u8, // <-- Nouvelle variable pour le curseur
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
            pourcentage_protection: 5, // Valeur par défaut (5%) comme dans la CLI
        }
    }
}

// --- INTERFACE GRAPHIQUE ---

impl eframe::App for AppCompresseur {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
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

        egui::CentralPanel::default().show(ctx, |ui| {
            ui.heading("Oxide Toolkit");
            ui.separator();

            ui.horizontal(|ui| {
                ui.selectable_value(&mut self.mode_actuel, Mode::Compresser, "📦 Compresser");
                ui.selectable_value(&mut self.mode_actuel, Mode::Extraire, "📂 Extraire");
                ui.selectable_value(&mut self.mode_actuel, Mode::Chiffrer, "🔒 Chiffrer");
                ui.selectable_value(&mut self.mode_actuel, Mode::Dechiffrer, "🔓 Déchiffrer");
                ui.selectable_value(&mut self.mode_actuel, Mode::Proteger, "🛡️ Protéger");
                ui.selectable_value(&mut self.mode_actuel, Mode::Reparer, "🛠️ Réparer");
            });
            ui.separator();

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
                }
            });

            if let Some(chemin) = &self.fichier_selectionne {
                ui.label(format!("Cible : {}", chemin.display()));
            } else {
                ui.label("Aucune cible sélectionnée.");
            }

            ui.add_space(10.0);

            // --- OPTIONS SPÉCIFIQUES ---
            if self.mode_actuel == Mode::Chiffrer || self.mode_actuel == Mode::Dechiffrer || self.mode_actuel == Mode::Extraire {
                ui.horizontal(|ui| {
                    ui.label("Mot de passe :");
                    ui.add(egui::TextEdit::singleline(&mut self.mot_de_passe).password(true));
                });
            }

            // AJOUT DU CURSEUR POUR LA PROTECTION
            if self.mode_actuel == Mode::Proteger {
                ui.group(|ui| {
                    ui.label("Niveau de redondance (Reed-Solomon) :");
                    // Le range est de 1 à 20% conformément aux spécifications d'Oxide
                    ui.add(egui::Slider::new(&mut self.pourcentage_protection, 1..=20).suffix("%"));
                    ui.label(egui::RichText::new("Plus le % est élevé, mieux l'archive résistera à la corruption, mais plus le fichier sera lourd.").small());
                });
            }

            ui.add_space(15.0);

            let texte_bouton = match self.mode_actuel {
                Mode::Compresser => "🚀 Lancer la compression",
                Mode::Extraire => "📂 Lancer l'extraction",
                Mode::Chiffrer => "🔒 Verrouiller l'archive",
                Mode::Dechiffrer => "🔓 Déverrouiller l'archive",
                Mode::Proteger => "🛡️ Appliquer la protection",
                Mode::Reparer => "🛠️ Tenter une réparation",
            };

            let btn_action = ui.add_enabled(
                self.fichier_selectionne.is_some() && !self.en_cours,
                egui::Button::new(texte_bouton)
            );

            if btn_action.clicked() {
                self.en_cours = true;
                self.progression = 0.0;
                self.message_fin = None;

                let (tx, rx) = mpsc::channel();
                self.receveur_msg = Some(rx);

                let ctx_clone = ctx.clone();
                let chemin_source = self.fichier_selectionne.clone().unwrap();
                let mode = self.mode_actuel;
                let mdp = self.mot_de_passe.clone();
                let recovery_pct = self.pourcentage_protection; // On capture la valeur choisie

                thread::spawn(move || {
                    let mut sink = EguiTelemetrySink { tx: tx.clone(), ctx: ctx_clone.clone() };
                    
                    let resultat = match mode {
                        Mode::Compresser => {
                            let mut chemin_dest = chemin_source.clone();
                            if chemin_source.is_dir() {
                                chemin_dest.set_file_name(format!("{}.oxz", chemin_source.file_name().unwrap().to_string_lossy()));
                            } else {
                                chemin_dest.set_extension("oxz");
                            }

                            let pool = Arc::new(BufferPool::new(64 * 1024 * 1024, 16));
                            let config = ArchivePipelineConfig::new(1024 * 1024, 4, pool, CompressionAlgo::Zstd);
                            let pipeline = ArchivePipeline::new(config);
                            let options = RunTelemetryOptions { progress_interval: Duration::from_millis(50), emit_final_progress: true, include_telemetry_snapshot: false };
                            
                            if let Ok(fichier_sortie) = File::create(&chemin_dest) {
                                let res = pipeline.archive_path_seekable(&chemin_source, fichier_sortie, options, Some(&mut sink));
                                if res.is_ok() { Ok(format!("Archivé vers {}", chemin_dest.display())) } else { Err("Erreur lors de la compression".to_string()) }
                            } else {
                                Err("Impossible de créer le fichier de destination".to_string())
                            }
                        },
                        Mode::Extraire => {
                            let mut chemin_dest = chemin_source.clone();
                            chemin_dest.set_extension(""); 
                            
                            let pool = Arc::new(BufferPool::new(64 * 1024 * 1024, 16));
                            let mut config = ArchivePipelineConfig::new(1024 * 1024, 4, pool, CompressionAlgo::Zstd);
                            if !mdp.is_empty() { config.password = Some(mdp); }
                            let pipeline = ArchivePipeline::new(config);
                            let options = RunTelemetryOptions { progress_interval: Duration::from_millis(50), emit_final_progress: true, include_telemetry_snapshot: false };
                            
                            if let Ok(fichier_entree) = File::open(&chemin_source) {
                                let res = pipeline.extract_path_file(fichier_entree, &chemin_dest, options, Some(&mut sink));
                                if res.is_ok() { Ok(format!("Extrait vers {}", chemin_dest.display())) } else { Err("Erreur d'extraction (mauvais mot de passe ?)".to_string()) }
                            } else { Err("Impossible de lire l'archive".to_string()) }
                        },
                        Mode::Chiffrer => {
                            let mut chemin_dest = chemin_source.clone();
                            chemin_dest.set_extension("enc.oxz");
                            match oxide_core::encrypt_existing_archive(&chemin_source, &chemin_dest, &mdp) {
                                Ok(_) => Ok("Archive chiffrée avec succès.".to_string()),
                                Err(_) => Err("Échec du chiffrement.".to_string()),
                            }
                        },
                        Mode::Dechiffrer => {
                            let mut chemin_dest = chemin_source.clone();
                            chemin_dest.set_extension("dec.oxz");
                            match oxide_core::decrypt_existing_archive(&chemin_source, &chemin_dest, &mdp) {
                                Ok(_) => Ok("Archive déchiffrée avec succès.".to_string()),
                                Err(_) => Err("Échec du déchiffrement.".to_string()),
                            }
                        },
                        Mode::Proteger => {
                            let mut chemin_dest = chemin_source.clone();
                            chemin_dest.set_extension("protected.oxz");
                            // UTILISATION DE LA VALEUR DU SLIDER ICI
                            match oxide_core::recovery::protect_existing_archive(&chemin_source, &chemin_dest, recovery_pct) {
                                Ok(_) => Ok(format!("Données de récupération ajoutées ({}%).", recovery_pct)),
                                Err(_) => Err("Échec de la protection.".to_string()),
                            }
                        },
                        Mode::Reparer => {
                            let mut chemin_dest = chemin_source.clone();
                            chemin_dest.set_extension("repaired.oxz");
                            match oxide_core::recovery::repair_corrupted_archive(&chemin_source, &chemin_dest) {
                                Ok(_) => Ok("Archive réparée avec succès !".to_string()),
                                Err(_) => Err("Impossible de réparer l'archive.".to_string()),
                            }
                        }
                    };

                    let _ = tx.send(AppMsg::Termine(resultat));
                    ctx_clone.request_repaint();
                });
            }

            ui.add_space(20.0);

            if self.en_cours || (self.progression > 0.0 && self.message_fin.is_none()) {
                let barre = egui::ProgressBar::new(self.progression)
                    .show_percentage()
                    .animate(self.en_cours);
                ui.add(barre);
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
        viewport: egui::ViewportBuilder::default()
            .with_inner_size([550.0, 380.0]) // Légère augmentation de la hauteur
            .with_resizable(false),
        ..Default::default()
    };

    eframe::run_native(
        "Oxide Toolkit",
        options,
        Box::new(|_cc| Box::<AppCompresseur>::default()),
    )
}