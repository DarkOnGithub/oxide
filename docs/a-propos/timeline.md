---
title: Chronologie des Fonctionnalités
description: Chronologie reconstituée du développement d'Oxide à partir du code et des commits
---

# Chronologie des Fonctionnalités

> Frise reconstituée à partir du code présent dans le workspace et de l'historique Git. Les dates restent cohérentes sur la période du 01/02 au 22/03, avec quelques regroupements pour garder une lecture claire.

<div class="feature-hierarchy">
<div class="feature-category">
<div class="category-label">⚡ Noyau d'exécution & mémoire</div>
<div class="sub-feature">
<div class="sub-feature-label">Runtime parallèle</div>
<div class="sub-sub-feature">
<div class="feature-meta">
<span class="feature-title">Queue work-stealing et worker pool</span>
<span class="feature-timeline">06/02 - 06/02</span>
</div>
<div class="feature-owner">Par <strong>Dark</strong></div>
</div>
<div class="sub-sub-feature">
<div class="feature-meta">
<span class="feature-title">Attente, notification et couverture de tests du scheduler</span>
<span class="feature-timeline">06/02 - 11/02</span>
</div>
<div class="feature-owner">Par <strong>Dark</strong></div>
</div>
</div>
<div class="sub-feature">
<div class="sub-feature-label">Mémoire & I/O</div>
<div class="sub-sub-feature">
<div class="feature-meta">
<span class="feature-title">BufferPool, PooledBuffer et MmapInput</span>
<span class="feature-timeline">06/02 - 06/02</span>
</div>
<div class="feature-owner">Par <strong>Dark</strong></div>
</div>
<div class="sub-sub-feature">
<div class="feature-meta">
<span class="feature-title">BatchData mappé, zéro-copie et conversion vers payloads réutilisables</span>
<span class="feature-timeline">06/02 - 16/03</span>
</div>
<div class="feature-owner">Par <strong>Dark</strong></div>
</div>
<div class="sub-sub-feature">
<div class="feature-meta">
<span class="feature-title">Scratch arenas locales par worker pour limiter les allocations</span>
<span class="feature-timeline">05/03 - 05/03</span>
</div>
<div class="feature-owner">Par <strong>Dark</strong></div>
</div>
</div>
<div class="sub-feature">
<div class="sub-feature-label">Détection & scan</div>
<div class="sub-sub-feature">
<div class="feature-meta">
<span class="feature-title">FormatDetector, heuristiques magic bytes et modes image/audio</span>
<span class="feature-timeline">06/02 - 06/02</span>
</div>
<div class="feature-owner">Par <strong>Dark</strong></div>
</div>
<div class="sub-sub-feature">
<div class="feature-meta">
<span class="feature-title">InputScanner, métriques de scan et découverte accélérée des fichiers</span>
<span class="feature-timeline">06/02 - 13/03</span>
</div>
<div class="feature-owner">Par <strong>Dark</strong></div>
</div>
</div>
</div>
<div class="feature-category">
<div class="category-label">🏗️ Format OXZ & pipeline</div>
<div class="sub-feature">
<div class="sub-feature-label">Format d'archive</div>
<div class="sub-sub-feature">
<div class="feature-meta">
<span class="feature-title">Headers, reader, writer et reordering OXZ</span>
<span class="feature-timeline">06/02 - 09/02</span>
</div>
<div class="feature-owner">Par <strong>Dark</strong></div>
</div>
<div class="sub-sub-feature">
<div class="feature-meta">
<span class="feature-title">Archive seekable, métadonnées canoniques et manifest enrichi</span>
<span class="feature-timeline">04/03 - 14/03</span>
</div>
<div class="feature-owner">Par <strong>Dark</strong></div>
</div>
<div class="sub-sub-feature">
<div class="feature-meta">
<span class="feature-title">Commande tree et listing arborescent fondé sur le manifest</span>
<span class="feature-timeline">08/03 - 14/03</span>
</div>
<div class="feature-owner">Par <strong>Dark</strong></div>
</div>
</div>
<div class="sub-feature">
<div class="sub-feature-label">Archivage</div>
<div class="sub-sub-feature">
<div class="feature-meta">
<span class="feature-title">Pipeline phase 1, roundtrip de base et archivage de répertoires</span>
<span class="feature-timeline">06/02 - 09/02</span>
</div>
<div class="feature-owner">Par <strong>Dark</strong></div>
</div>
<div class="sub-sub-feature">
<div class="feature-meta">
<span class="feature-title">Préflight de découverte, jwalk, lectures parallèles et mémoire bornée</span>
<span class="feature-timeline">07/02 - 13/03</span>
</div>
<div class="feature-owner">Par <strong>Dark</strong></div>
</div>
<div class="sub-sub-feature">
<div class="feature-meta">
<span class="feature-title">Réglages inflight, producer threads et modularisation de l'archiver</span>
<span class="feature-timeline">09/03 - 13/03</span>
</div>
<div class="feature-owner">Par <strong>Dark</strong></div>
</div>
</div>
<div class="sub-feature">
<div class="sub-feature-label">Extraction</div>
<div class="sub-sub-feature">
<div class="feature-meta">
<span class="feature-title">Commande extract, restauration file/dir et rapports détaillés</span>
<span class="feature-timeline">07/02 - 11/02</span>
</div>
<div class="feature-owner">Par <strong>Dark</strong></div>
</div>
<div class="sub-sub-feature">
<div class="feature-meta">
<span class="feature-title">Extraction streaming, bounded reorder writer et filtres de chemins</span>
<span class="feature-timeline">04/03 - 14/03</span>
</div>
<div class="feature-owner">Par <strong>Dark</strong></div>
</div>
</div>
</div>
<div class="feature-category">
<div class="category-label">🖥️ CLI, reporting & observabilité</div>
<div class="sub-feature">
<div class="sub-feature-label">Interface terminal</div>
<div class="sub-sub-feature">
<div class="feature-meta">
<span class="feature-title">Archive CLI avec progression live, stats workers et vitesses disque</span>
<span class="feature-timeline">06/02 - 12/02</span>
</div>
<div class="feature-owner">Par <strong>Dark</strong></div>
</div>
<div class="sub-sub-feature">
<div class="feature-meta">
<span class="feature-title">Refonte du CLI, presets JSON et flags avancés archive/extract</span>
<span class="feature-timeline">12/03 - 15/03</span>
</div>
<div class="feature-owner">Par <strong>Dark</strong></div>
</div>
</div>
<div class="sub-feature">
<div class="sub-feature-label">Télémétrie</div>
<div class="sub-sub-feature">
<div class="feature-meta">
<span class="feature-title">Système d'événements, snapshots mémoire et métriques workers</span>
<span class="feature-timeline">06/02 - 12/02</span>
</div>
<div class="feature-owner">Par <strong>Dark</strong></div>
</div>
<div class="sub-sub-feature">
<div class="feature-meta">
<span class="feature-title">Throughput, wall-clock, métriques préprocessing/compression et rapports</span>
<span class="feature-timeline">11/02 - 12/03</span>
</div>
<div class="feature-owner">Par <strong>Dark</strong></div>
</div>
</div>
<div class="sub-feature">
<div class="sub-feature-label">Phase UI repliée</div>
<div class="sub-sub-feature">
<div class="feature-meta">
<span class="feature-title">Prototype oxide-tui, puis recentrage vers un CLI plus léger</span>
<span class="feature-timeline">11/02 - 12/03</span>
</div>
<div class="feature-owner">Par <strong>Dark</strong></div>
</div>
</div>
</div>
<div class="feature-category">
<div class="category-label">📦 Compression & intégrité</div>
<div class="sub-feature">
<div class="sub-feature-label">Codecs retenus</div>
<div class="sub-sub-feature">
<div class="feature-meta">
<span class="feature-title">LZ4 maison, fast paths de décodage et bancs de perf dédiés</span>
<span class="feature-timeline">10/02 - 05/03</span>
</div>
<div class="feature-owner">Par <strong>Dark</strong></div>
</div>
<div class="sub-sub-feature">
<div class="feature-meta">
<span class="feature-title">Intégration Zstandard, niveau configurable et support tar</span>
<span class="feature-timeline">13/03 - 18/03</span>
</div>
<div class="feature-owner">Par <strong>Dark</strong></div>
</div>
</div>
<div class="sub-feature">
<div class="sub-feature-label">Stockage intelligent</div>
<div class="sub-sub-feature">
<div class="feature-meta">
<span class="feature-title">Skip des blocs non rentables, raw passthrough et stockage brut ciblé</span>
<span class="feature-timeline">12/02 - 16/03</span>
</div>
<div class="feature-owner">Par <strong>Dark</strong></div>
</div>
<div class="sub-sub-feature">
<div class="feature-meta">
<span class="feature-title">Module checksum et validation d'intégrité par bloc</span>
<span class="feature-timeline">17/03 - 20/03</span>
</div>
<div class="feature-owner">Par <strong>Dark</strong> et <strong>Romain</strong></div>
</div>
</div>
<div class="sub-feature">
<div class="sub-feature-label">Phases transitoires</div>
<div class="sub-sub-feature">
<div class="feature-meta">
<span class="feature-title">Passage temporaire par Deflate et LZMA avant recentrage LZ4/Zstd</span>
<span class="feature-timeline">09/02 - 05/03</span>
</div>
<div class="feature-owner">Par <strong>Dark</strong></div>
</div>
</div>
</div>
<div class="feature-category">
<div class="category-label">🖼️ Prétraitements adaptés au contenu</div>
<div class="sub-feature">
<div class="sub-feature-label">Base multi-domaines</div>
<div class="sub-sub-feature">
<div class="feature-meta">
<span class="feature-title">Stratégies texte, image, audio et binaire intégrées au core</span>
<span class="feature-timeline">05/02 - 09/02</span>
</div>
<div class="feature-owner">Par <strong>Dark</strong></div>
</div>
<div class="sub-sub-feature">
<div class="feature-meta">
<span class="feature-title">Métadonnées de prétraitement, conversions et choix selon le format</span>
<span class="feature-timeline">17/02 - 19/02</span>
</div>
<div class="feature-owner">Par <strong>Dark</strong></div>
</div>
</div>
<div class="sub-feature">
<div class="sub-feature-label">Texte & binaire</div>
<div class="sub-sub-feature">
<div class="feature-meta">
<span class="feature-title">BCJ et BPE pour exécutables et corpus textuels</span>
<span class="feature-timeline">05/02 - 17/02</span>
</div>
<div class="feature-owner">Par <strong>Dark</strong></div>
</div>
<div class="sub-sub-feature">
<div class="feature-meta">
<span class="feature-title">BWT ajouté plus tard et retenu comme stratégie texte principale</span>
<span class="feature-timeline">18/03 - 18/03</span>
</div>
<div class="feature-owner">Par <strong>Alexandre J. L. Salgueiro</strong></div>
</div>
</div>
<div class="sub-feature">
<div class="sub-feature-label">Image & audio</div>
<div class="sub-sub-feature">
<div class="feature-meta">
<span class="feature-title">LPC audio, Paeth et LOCO-I</span>
<span class="feature-timeline">05/02 - 19/02</span>
</div>
<div class="feature-owner">Par <strong>Dark</strong></div>
</div>
<div class="sub-sub-feature">
<div class="feature-meta">
<span class="feature-title">YCoCg-R, validation progressive et intégration dans le pipeline</span>
<span class="feature-timeline">13/03 - 17/03</span>
</div>
<div class="feature-owner">Par <strong>Alexandre J. L. Salgueiro</strong> et <strong>Dark</strong></div>
</div>
</div>
</div>
<div class="feature-category">
<div class="category-label">🧪 Expérimentations & refontes</div>
<div class="sub-feature">
<div class="sub-feature-label">Pipeline expérimental</div>
<div class="sub-sub-feature">
<div class="feature-meta">
<span class="feature-title">Chunking piloté par planner et encodage aware des dictionnaires</span>
<span class="feature-timeline">06/03 - 09/03</span>
</div>
<div class="feature-owner">Par <strong>Dark</strong></div>
</div>
<div class="sub-sub-feature">
<div class="feature-meta">
<span class="feature-title">Retrait du planner et simplification du pipeline de production</span>
<span class="feature-timeline">09/03 - 13/03</span>
</div>
<div class="feature-owner">Par <strong>Dark</strong></div>
</div>
</div>
<div class="sub-feature">
<div class="sub-feature-label">Extraction avancée</div>
<div class="sub-sub-feature">
<div class="feature-meta">
<span class="feature-title">Passage par une extraction orientée DAG avant stabilisation</span>
<span class="feature-timeline">04/03 - 05/03</span>
</div>
<div class="feature-owner">Par <strong>Dark</strong></div>
</div>
</div>
</div>
<div class="feature-category">
<div class="category-label">🛠️ Outils, benchmarks & documentation</div>
<div class="sub-feature">
<div class="sub-feature-label">Qualité de projet</div>
<div class="sub-sub-feature">
<div class="feature-meta">
<span class="feature-title">Tests unitaires, roundtrips OXZ, couverture scanner et pipeline</span>
<span class="feature-timeline">06/02 - 13/02</span>
</div>
<div class="feature-owner">Par <strong>Dark</strong></div>
</div>
<div class="sub-sub-feature">
<div class="feature-meta">
<span class="feature-title">GitHub Actions Rust, clippy et nettoyage régulier du code</span>
<span class="feature-timeline">09/02 - 10/03</span>
</div>
<div class="feature-owner">Par <strong>Dark</strong></div>
</div>
</div>
<div class="sub-feature">
<div class="sub-feature-label">Benchmarks</div>
<div class="sub-sub-feature">
<div class="feature-meta">
<span class="feature-title">Bench scanner, LZ4, throughput archive et comparaison mksquashfs</span>
<span class="feature-timeline">06/02 - 15/03</span>
</div>
<div class="feature-owner">Par <strong>Dark</strong></div>
</div>
<div class="sub-sub-feature">
<div class="feature-meta">
<span class="feature-title">Bencher étendu aux passes multiples, presets et extraction</span>
<span class="feature-timeline">13/03 - 15/03</span>
</div>
<div class="feature-owner">Par <strong>Dark</strong></div>
</div>
</div>
<div class="sub-feature">
<div class="sub-feature-label">Documentation</div>
<div class="sub-sub-feature">
<div class="feature-meta">
<span class="feature-title">Docs techniques d'architecture, format et preprocessing</span>
<span class="feature-timeline">05/02 - 19/02</span>
</div>
<div class="feature-owner">Par <strong>Dark</strong></div>
</div>
<div class="sub-sub-feature">
<div class="feature-meta">
<span class="feature-title">Site VitePress, restructuration des pages et déploiement GitHub Pages</span>
<span class="feature-timeline">18/03 - 22/03</span>
</div>
<div class="feature-owner">Par <strong>Dark</strong></div>
</div>
</div>
</div>
</div>
