---
title: Dépendances
description: Bibliothèques open-source ayant permis de construire Oxide
---

# Dépendances

Oxide repose sur de nombreuses briques open-source performantes de l'écosystème Rust et Web.

## Cœur et Compression (`oxide-core`)

La librairie centrale gérant la compression et les algorithmes fait appel aux paquets suivants :

- **Concurrence & Multithreading** : 
  - `crossbeam-channel` & `crossbeam-deque` : Structures de données performantes pour la communication entre nos nombreux threads.
  - `num_cpus` : Outil permettant de sonder dynamiquement le nombre de cœurs processeurs disponibles pour adapter la parallélisation.
  
- **Gestion Mémoire & Entrées/Sorties (I/O)** :
  - `memmap2` : Lecture de fichiers mappés en mémoire (mmap) pour limiter les copies inutiles et accélérer drastiquement les traitements.
  - `bytes` & `memchr` : Utilitaires pour la manipulation rapide de buffers binaires orientés réseau.
  - `jwalk` : Traitement optimisé à grande vitesse des parcours récursifs de répertoires.
  - `libc` : Couche d'interaction très bas-niveau avec le système d'exploitation.

- **Filtres et Détection de Contenu** :
  - `infer` : Inférence magique et ultra-rapide des formats de fichiers à partir de leur en-tête.
  - `image` & `symphonia` : Bibliothèques expertes pour extraire ou valider les données brutes des médias (images et audio).
  - `regex` : Moteur ultra-rapide pour l'analyse textuelle avancée.

- **Utilitaires Génériques** :
  - `serde` : Standard de sérialisation et dé-sérialisation des données.
  - `anyhow` & `thiserror` : Structures modernes pour la gestion fine et traçable des erreurs et exceptions.
  - `tracing` : Instrumentations et logs asynchrones de l'application.

## Interface Ligne de Commande (`oxide`)

L'utilitaire `oxide` fournit une expérience utilisateur puissante et agréable grâce à :

- `clap` : Le standard absolu en Rust pour parser et valider avec fiabilité les commandes et arguments du terminal.
- `nu-ansi-term` & `terminal_size` : Affichage de textes formatés, adaptés de manière dynamique à la largeur de l'écran du terminal.
- `serde_json` : Émission de la télémétrie et log au format JSON si demandé.
- `tracing-subscriber` : Routeur et formateur performant des logs à l'écran.

## Site de Documentation (Web)

Toute cette documentation est générée et propulsée par un écosystème web moderne :

- **VitePress & Vue.js** : Générateur de site statique ultra-rapide (SSG) de nouvelle génération, permettant une navigation fluide (type SPA) et l'intégration de design personnalisés via d'élégants composants Vue.

## Identité Visuelle

- L'esthétique de notre **logo** s'inspire du travail de [SAWARATSUKI / KawaiiLogos](https://github.com/SAWARATSUKI/KawaiiLogos).
