use eframe::egui;
use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::mpsc::{self, Receiver, Sender};
use std::thread;
use std::time::{Duration, Instant};

use oxide_core::telemetry::{RunTelemetryOptions, TelemetryEvent, TelemetrySink};
use oxide_core::{ArchivePipeline, ArchivePipelineConfig, BufferPool, CompressionAlgo};

const BASE_WINDOW_WIDTH: f32 = 720.0;
const BASE_WINDOW_HEIGHT: f32 = 480.0;
const MIN_UI_SCALE: f32 = 1.10;
const MAX_UI_SCALE: f32 = 1.55;

fn update_ui_scale(ctx: &egui::Context) {
    let zoom_factor = ctx.zoom_factor();
    let unscaled_window_size = ctx.input(|i| {
        let scaled_size = i
            .viewport()
            .inner_rect
            .map(|rect| rect.size())
            .unwrap_or_else(|| i.screen_rect().size());

        scaled_size * zoom_factor
    });

    let scale = (unscaled_window_size.x / BASE_WINDOW_WIDTH)
        .min(unscaled_window_size.y / BASE_WINDOW_HEIGHT)
        .clamp(MIN_UI_SCALE, MAX_UI_SCALE);

    if (zoom_factor - scale).abs() > 0.01 {
        ctx.set_zoom_factor(scale);
    }
}

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

    compression_chiffrer: bool,
    compression_proteger: bool,

    mot_de_passe: String,
    confirmation_mot_de_passe: String,
    pourcentage_protection: u8,

    dialogue_ouvert: bool,
    receveur_dialogue: Option<Receiver<Option<PathBuf>>>,

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

            compression_chiffrer: false,
            compression_proteger: false,

            mot_de_passe: String::new(),
            confirmation_mot_de_passe: String::new(),
            pourcentage_protection: 5,

            dialogue_ouvert: false,
            receveur_dialogue: None,

            fenetre_options_ouverte: false,
            config_algo: CompressionAlgo::Zstd,
            config_block_size_kb: 2048,
            config_workers: thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(4),
        }
    }
}

impl eframe::App for AppCompresseur {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        update_ui_scale(ctx);

        let (plein_ecran, basculer_plein_ecran, quitter_plein_ecran) = ctx.input(|i| {
            let plein_ecran = i.viewport().fullscreen.unwrap_or(false);
            (
                plein_ecran,
                i.key_pressed(egui::Key::F11),
                plein_ecran && i.key_pressed(egui::Key::Escape),
            )
        });
        if basculer_plein_ecran {
            ctx.send_viewport_cmd(egui::ViewportCommand::Fullscreen(!plein_ecran));
        } else if quitter_plein_ecran {
            ctx.send_viewport_cmd(egui::ViewportCommand::Fullscreen(false));
        }

        if let Some(rx) = &self.receveur_dialogue {
            if let Ok(resultat) = rx.try_recv() {
                self.dialogue_ouvert = false;
                if let Some(chemin) = resultat {
                    self.fichier_selectionne = Some(chemin);
                    self.message_fin = None;
                    self.progression = 0.0;
                }
            }
        }

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
            egui::Window::new("Profil de Compression")
                .collapsible(false)
                .resizable(false)
                .show(ctx, |ui| {
                    ui.label("Choisissez un niveau de performance :");
                    ui.add_space(10.0);

                    ui.horizontal(|ui| {
                        if ui.button("🚀 Fast").clicked() {
                            self.config_algo = CompressionAlgo::Lz4;
                            self.config_block_size_kb = 3072;
                            self.config_workers = thread::available_parallelism()
                                .map(|n| n.get())
                                .unwrap_or(4);
                        }
                        if ui.button("⚖️ Balanced").clicked() {
                            self.config_algo = CompressionAlgo::Zstd;
                            self.config_block_size_kb = 2048;
                            self.config_workers = thread::available_parallelism()
                                .map(|n| n.get())
                                .unwrap_or(4);
                        }
                        if ui.button("💎 Ultra").clicked() {
                            self.config_algo = CompressionAlgo::Lzma;
                            self.config_block_size_kb = 3072;
                            self.config_workers = thread::available_parallelism()
                                .map(|n| n.get())
                                .unwrap_or(4);
                        }
                    });

                    ui.add_space(15.0);
                    let nom_preset = match self.config_algo {
                        CompressionAlgo::Lz4 => "Fast (Optimisé pour la vitesse)",
                        CompressionAlgo::Zstd => "Balanced (Compromis idéal)",
                        CompressionAlgo::Lzma => "Ultra (Taille minimale)",
                    };
                    ui.label(egui::RichText::new(format!("Actuel : {}", nom_preset)).italics());

                    ui.add_space(15.0);
                    if ui.button("✅ Valider et Fermer").clicked() {
                        self.fenetre_options_ouverte = false;
                    }
                });
        }

        // --- PANEL PRINCIPAL ---
        egui::CentralPanel::default().show(ctx, |ui| {
            ui.heading("Oxide Toolkit");
            ui.separator();

            // Mémorisation du mode actuel AVANT le choix de l'utilisateur
            let mode_precedent = self.mode_actuel;

            ui.horizontal(|ui| {
                ui.selectable_value(&mut self.mode_actuel, Mode::Compresser, "📦 Compresser");
                ui.selectable_value(&mut self.mode_actuel, Mode::Extraire, "📂 Extraire");
                ui.selectable_value(&mut self.mode_actuel, Mode::Chiffrer, "🔒 Chiffrer");
                ui.selectable_value(&mut self.mode_actuel, Mode::Dechiffrer, "🔓 Déchiffrer");
                ui.selectable_value(&mut self.mode_actuel, Mode::Proteger, "🛡️ Protéger");
                ui.selectable_value(&mut self.mode_actuel, Mode::Verifier, "🔍 Vérifier");
                ui.selectable_value(&mut self.mode_actuel, Mode::Reparer, "🛠️ Réparer");
            });
            ui.separator();

            // Si l'utilisateur vient de cliquer sur un onglet différent
            if self.mode_actuel != mode_precedent {
                self.mot_de_passe.clear();
                self.confirmation_mot_de_passe.clear();
                self.message_fin = None; // On nettoie les anciens messages
                self.progression = 0.0; // On remet la barre à 0
            }

            ui.horizontal(|ui| {
                if ui
                    .add_enabled(
                        !self.en_cours && !self.dialogue_ouvert,
                        egui::Button::new("📄 Choisir un fichier"),
                    )
                    .clicked()
                {
                    self.dialogue_ouvert = true;
                    let (tx, rx) = mpsc::channel();
                    self.receveur_dialogue = Some(rx);
                    let ctx_clone = ctx.clone();
                    thread::spawn(move || {
                        let chemin = rfd::FileDialog::new().pick_file();
                        let _ = tx.send(chemin);
                        ctx_clone.request_repaint();
                    });
                }

                if self.mode_actuel == Mode::Compresser {
                    if ui
                        .add_enabled(
                            !self.en_cours && !self.dialogue_ouvert,
                            egui::Button::new("📁 Choisir un dossier"),
                        )
                        .clicked()
                    {
                        self.dialogue_ouvert = true;
                        let (tx, rx) = mpsc::channel();
                        self.receveur_dialogue = Some(rx);
                        let ctx_clone = ctx.clone();
                        thread::spawn(move || {
                            let chemin = rfd::FileDialog::new().pick_folder();
                            let _ = tx.send(chemin);
                            ctx_clone.request_repaint();
                        });
                    }

                    if ui.button("⚙️ Options").clicked() {
                        self.fenetre_options_ouverte = true;
                    }
                }
            });

            if self.dialogue_ouvert {
                ui.label(
                    egui::RichText::new("⏳ En attente de l'explorateur de fichiers...").weak(),
                );
            } else if let Some(chemin) = &self.fichier_selectionne {
                ui.label(
                    egui::RichText::new(format!("Cible : {}", chemin.display()))
                        .italics()
                        .weak(),
                );
            } else {
                ui.label(egui::RichText::new("Aucune cible sélectionnée.").weak());
            }

            ui.add_space(10.0);

            // --- OPTIONS PAR MODE ---
            if self.mode_actuel == Mode::Compresser {
                ui.group(|ui| {
                    ui.checkbox(
                        &mut self.compression_chiffrer,
                        "🔒 Protéger l'archive avec un mot de passe",
                    );
                    if self.compression_chiffrer {
                        ui.horizontal(|ui| {
                            ui.label("Mot de passe :");
                            ui.add(
                                egui::TextEdit::singleline(&mut self.mot_de_passe).password(true),
                            );
                        });
                        ui.horizontal(|ui| {
                            ui.label("Confirmer :");
                            ui.add(
                                egui::TextEdit::singleline(&mut self.confirmation_mot_de_passe)
                                    .password(true),
                            );
                        });

                        if !self.confirmation_mot_de_passe.is_empty()
                            && self.mot_de_passe != self.confirmation_mot_de_passe
                        {
                            ui.colored_label(
                                egui::Color32::from_rgb(255, 100, 100),
                                "⚠ Les mots de passe ne correspondent pas",
                            );
                        }
                    }

                    ui.add_space(5.0);

                    ui.checkbox(
                        &mut self.compression_proteger,
                        "🛡️ Ajouter des données de récupération",
                    );
                    if self.compression_proteger {
                        ui.horizontal(|ui| {
                            ui.label("Niveau de redondance :");
                            ui.add(
                                egui::Slider::new(&mut self.pourcentage_protection, 1..=20)
                                    .suffix("%"),
                            );
                        });
                    }
                });
            } else if self.mode_actuel == Mode::Chiffrer {
                ui.horizontal(|ui| {
                    ui.label("Mot de passe :");
                    ui.add(egui::TextEdit::singleline(&mut self.mot_de_passe).password(true));
                });
                ui.horizontal(|ui| {
                    ui.label("Confirmer :");
                    ui.add(
                        egui::TextEdit::singleline(&mut self.confirmation_mot_de_passe)
                            .password(true),
                    );
                });

                if !self.confirmation_mot_de_passe.is_empty()
                    && self.mot_de_passe != self.confirmation_mot_de_passe
                {
                    ui.colored_label(
                        egui::Color32::from_rgb(255, 100, 100),
                        "⚠ Les mots de passe ne correspondent pas",
                    );
                }
            } else if self.mode_actuel == Mode::Dechiffrer
                || self.mode_actuel == Mode::Extraire
                || self.mode_actuel == Mode::Verifier
            {
                ui.horizontal(|ui| {
                    if self.mode_actuel == Mode::Dechiffrer {
                        ui.label("Mot de passe :");
                    } else {
                        ui.label("Mot de passe (optionnel) :");
                    }
                    ui.add(egui::TextEdit::singleline(&mut self.mot_de_passe).password(true));
                });
            } else if self.mode_actuel == Mode::Proteger {
                ui.group(|ui| {
                    ui.label("Niveau de redondance :");
                    ui.add(egui::Slider::new(&mut self.pourcentage_protection, 1..=20).suffix("%"));
                    ui.label(
                        egui::RichText::new(
                            "Plus le % est élevé, mieux l'archive résistera à la corruption.",
                        )
                        .small(),
                    );
                });
            }

            ui.add_space(15.0);

            // Validation de sécurité pour le mot de passe
            let mdp_valide = match self.mode_actuel {
                Mode::Compresser => {
                    if self.compression_chiffrer {
                        !self.mot_de_passe.is_empty()
                            && self.mot_de_passe == self.confirmation_mot_de_passe
                    } else {
                        true
                    }
                }
                Mode::Chiffrer => {
                    !self.mot_de_passe.is_empty()
                        && self.mot_de_passe == self.confirmation_mot_de_passe
                }
                Mode::Dechiffrer => !self.mot_de_passe.is_empty(),
                _ => true,
            };

            let texte_bouton = match self.mode_actuel {
                Mode::Compresser => "🚀 Lancer la création de l'archive",
                Mode::Extraire => "📂 Lancer l'extraction",
                Mode::Chiffrer => "🔒 Verrouiller l'archive",
                Mode::Dechiffrer => "🔓 Déverrouiller l'archive",
                Mode::Proteger => "🛡️ Appliquer la protection",
                Mode::Verifier => "🔍 Vérifier l'intégrité",
                Mode::Reparer => "🛠️ Tenter une réparation",
            };

            let btn_action = ui.add_enabled(
                self.fichier_selectionne.is_some()
                    && !self.en_cours
                    && !self.dialogue_ouvert
                    && mdp_valide,
                egui::Button::new(texte_bouton),
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
                let recovery_pct = self.pourcentage_protection;

                let compression_chiffrer = self.compression_chiffrer;
                let compression_proteger = self.compression_proteger;

                let algo = self.config_algo;
                let block_size = self.config_block_size_kb * 1024;
                let workers = self.config_workers;

                thread::spawn(move || {
                    let mut sink = EguiTelemetrySink {
                        tx: tx.clone(),
                        ctx: ctx_clone.clone(),
                    };

                    let resultat = match mode {
                        Mode::Compresser => {
                            let mut run_compression = || -> Result<String, String> {
                                let mut chemin_dest = chemin_source.clone();
                                if chemin_source.is_dir() {
                                    chemin_dest.set_file_name(format!(
                                        "{}.oxz",
                                        chemin_source.file_name().unwrap().to_string_lossy()
                                    ));
                                } else {
                                    chemin_dest.set_extension("oxz");
                                }

                                let pool = Arc::new(BufferPool::new(64 * 1024 * 1024, 8));
                                let config =
                                    ArchivePipelineConfig::new(block_size, workers, pool, algo);
                                let pipeline = ArchivePipeline::new(config);
                                let options = RunTelemetryOptions {
                                    progress_interval: Duration::from_millis(50),
                                    emit_final_progress: true,
                                    include_telemetry_snapshot: false,
                                };

                                let fichier_sortie = File::create(&chemin_dest)
                                    .map_err(|_| "Impossible de créer le fichier".to_string())?;

                                if pipeline
                                    .archive_path_seekable(
                                        &chemin_source,
                                        fichier_sortie,
                                        options,
                                        Some(&mut sink),
                                    )
                                    .is_err()
                                {
                                    return Err("Erreur lors de la compression".to_string());
                                }

                                if compression_chiffrer {
                                    let temp_enc = chemin_dest.with_extension("oxz.enc");
                                    if oxide_core::encrypt_existing_archive(
                                        &chemin_dest,
                                        &temp_enc,
                                        &mdp,
                                    )
                                    .is_ok()
                                    {
                                        let _ = std::fs::rename(&temp_enc, &chemin_dest);
                                    } else {
                                        return Err(
                                            "L'archive a été créée mais le chiffrement a échoué."
                                                .to_string(),
                                        );
                                    }
                                }

                                if compression_proteger {
                                    let temp_prot = chemin_dest.with_extension("oxz.prot");
                                    if oxide_core::recovery::protect_existing_archive(
                                        &chemin_dest,
                                        &temp_prot,
                                        recovery_pct,
                                    )
                                    .is_ok()
                                    {
                                        let _ = std::fs::rename(&temp_prot, &chemin_dest);
                                    } else {
                                        return Err(
                                            "L'archive a été créée mais la protection a échoué."
                                                .to_string(),
                                        );
                                    }
                                }

                                Ok(format!(
                                    "Archivé avec succès ({}).",
                                    match algo {
                                        CompressionAlgo::Zstd => "Zstd",
                                        CompressionAlgo::Lz4 => "Lz4",
                                        CompressionAlgo::Lzma => "Lzma",
                                    }
                                ))
                            };
                            run_compression()
                        }
                        Mode::Extraire => {
                            let mut chemin_dest = chemin_source.clone();
                            chemin_dest.set_extension("");

                            let pool = Arc::new(BufferPool::new(64 * 1024 * 1024, 8));
                            let mut config = ArchivePipelineConfig::new(
                                1024 * 1024,
                                4,
                                pool,
                                CompressionAlgo::Zstd,
                            );
                            if !mdp.is_empty() {
                                config.password = Some(mdp);
                            }
                            let pipeline = ArchivePipeline::new(config);
                            let options = RunTelemetryOptions {
                                progress_interval: Duration::from_millis(50),
                                emit_final_progress: true,
                                include_telemetry_snapshot: false,
                            };

                            if let Ok(fichier_entree) = File::open(&chemin_source) {
                                if pipeline
                                    .extract_path_file(
                                        fichier_entree,
                                        &chemin_dest,
                                        options,
                                        Some(&mut sink),
                                    )
                                    .is_ok()
                                {
                                    Ok(format!("Extrait vers {}", chemin_dest.display()))
                                } else {
                                    Err("Erreur d'extraction (mauvais mot de passe ?)".to_string())
                                }
                            } else {
                                Err("Impossible de lire l'archive".to_string())
                            }
                        }
                        Mode::Chiffrer => {
                            let mut chemin_dest = chemin_source.clone();
                            chemin_dest.set_extension("enc.oxz");
                            match oxide_core::encrypt_existing_archive(
                                &chemin_source,
                                &chemin_dest,
                                &mdp,
                            ) {
                                Ok(_) => Ok("Archive chiffrée avec succès.".to_string()),
                                Err(_) => Err("Échec du chiffrement.".to_string()),
                            }
                        }
                        Mode::Dechiffrer => {
                            let mut chemin_dest = chemin_source.clone();
                            chemin_dest.set_extension("dec.oxz");
                            match oxide_core::decrypt_existing_archive(
                                &chemin_source,
                                &chemin_dest,
                                &mdp,
                            ) {
                                Ok(_) => Ok("Archive déchiffrée avec succès.".to_string()),
                                Err(_) => Err("Échec du déchiffrement.".to_string()),
                            }
                        }
                        Mode::Proteger => {
                            let mut chemin_dest = chemin_source.clone();
                            chemin_dest.set_extension("protected.oxz");
                            match oxide_core::recovery::protect_existing_archive(
                                &chemin_source,
                                &chemin_dest,
                                recovery_pct,
                            ) {
                                Ok(_) => Ok(format!(
                                    "Données de récupération ajoutées ({}%).",
                                    recovery_pct
                                )),
                                Err(_) => Err("Échec de la protection.".to_string()),
                            }
                        }
                        Mode::Reparer => {
                            let mut chemin_dest = chemin_source.clone();
                            chemin_dest.set_extension("repaired.oxz");
                            match oxide_core::recovery::repair_corrupted_archive(
                                &chemin_source,
                                &chemin_dest,
                            ) {
                                Ok(_) => Ok("Archive réparée avec succès !".to_string()),
                                Err(_) => Err("Impossible de réparer l'archive.".to_string()),
                            }
                        }
                        Mode::Verifier => {
                            let verifier_archive = || -> Result<String, String> {
                                let fichier = File::open(&chemin_source)
                                    .map_err(|_| "Impossible d'ouvrir le fichier.".to_string())?;
                                let mut reader = oxide_core::format::ArchiveReader::new(fichier)
                                    .map_err(|_| "Fichier non valide.".to_string())?;
                                if !mdp.is_empty() {
                                    reader = reader
                                        .with_password(Some(mdp.clone()))
                                        .map_err(|_| "Mot de passe incorrect.".to_string())?;
                                }

                                let total = reader.block_count();
                                let mut erreurs = 0;
                                let mut dernier_rafraichissement = Instant::now();

                                for i in 0..total {
                                    if reader.read_block(i).is_err() {
                                        erreurs += 1;
                                    }

                                    if dernier_rafraichissement.elapsed()
                                        > Duration::from_millis(50)
                                    {
                                        let _ =
                                            tx.send(AppMsg::Progression(i as f32 / total as f32));
                                        ctx_clone.request_repaint();
                                        dernier_rafraichissement = Instant::now();
                                    }
                                }
                                let _ = tx.send(AppMsg::Progression(1.0));
                                ctx_clone.request_repaint();

                                if erreurs == 0 {
                                    Ok(format!("Archive saine ! {} blocs valides.", total))
                                } else {
                                    Err(format!("Archive corrompue : {} erreurs.", erreurs))
                                }
                            };
                            verifier_archive()
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
            .with_inner_size([BASE_WINDOW_WIDTH, BASE_WINDOW_HEIGHT])
            .with_min_inner_size([BASE_WINDOW_WIDTH, BASE_WINDOW_HEIGHT])
            .with_resizable(true),
        ..Default::default()
    };
    eframe::run_native(
        "Oxide Toolkit",
        options,
        Box::new(|_cc| Box::<AppCompresseur>::default()),
    )
}
