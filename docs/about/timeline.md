---
title: Chronologie du projet
description: Frise indicative du développement d'Oxide à partir des contributions réellement visibles dans le dépôt
---

# Chronologie du projet

> Cette frise reprend les grands chantiers de `docs/about/index.md` qui sont réellement visibles dans le dépôt. Les dates restent indicatives et servent surtout à restituer un déroulé plausible du projet entre février et mars 2026.

<div class="feature-hierarchy">
<div class="feature-category">
<div class="category-label">Compression et format</div>
<div class="sub-feature">
<div class="sub-feature-label">Compression</div>
<div class="sub-sub-feature">
<div class="feature-meta">
<span class="feature-title">Compression rapide avec LZ4</span>
<span class="feature-timeline">10/02 - 12/02</span>
</div>
<div class="feature-owner">Par <strong>Tom Huynh</strong></div>
</div>
<div class="sub-sub-feature">
<div class="feature-meta">
<span class="feature-title">Implémentation du format OXZ</span>
<span class="feature-timeline">À partir du 06/02</span>
</div>
<div class="feature-owner">Par <strong>Tom Huynh</strong></div>
</div>
 </div>
<div class="sub-feature">
<div class="sub-feature-label">Format et pipeline</div>
<div class="sub-sub-feature">
<div class="feature-meta">
<span class="feature-title">Mise en place du pipeline d'archivage</span>
<span class="feature-timeline">À partir du 06/02</span>
</div>
<div class="feature-owner">Par <strong>Tom Huynh</strong></div>
</div>
<div class="sub-sub-feature">
<div class="feature-meta">
<span class="feature-title">Travail sur l'exécution parallèle et le work stealing</span>
<span class="feature-timeline">06/02 - 11/02</span>
</div>
<div class="feature-owner">Par <strong>Tom Huynh</strong></div>
</div>
<div class="sub-sub-feature">
<div class="feature-meta">
<span class="feature-title">Mise en place des buffers réutilisables et des lectures en mémoire</span>
<span class="feature-timeline">06/02 - 16/03</span>
</div>
<div class="feature-owner">Par <strong>Tom Huynh</strong></div>
</div>
<div class="sub-sub-feature">
<div class="feature-meta">
<span class="feature-title">Organisation du scan, du manifest et de la lecture des archives</span>
<span class="feature-timeline">06/02 - 14/03</span>
</div>
<div class="feature-owner">Par <strong>Tom Huynh</strong></div>
</div>
</div>
</div>

<div class="feature-category">
<div class="category-label">Interface et usages</div>
<div class="sub-feature">
<div class="sub-feature-label">Commandes CLI</div>
<div class="sub-sub-feature">
<div class="feature-meta">
<span class="feature-title">Ajout de la commande archive</span>
<span class="feature-timeline">06/02 - 09/02</span>
</div>
<div class="feature-owner">Par <strong>Willian Huang Hong</strong></div>
</div>
<div class="sub-sub-feature">
<div class="feature-meta">
<span class="feature-title">Ajout de la commande extract</span>
<span class="feature-timeline">09/02 - 12/02</span>
</div>
<div class="feature-owner">Par <strong>Willian Huang Hong</strong></div>
</div>
<div class="sub-sub-feature">
<div class="feature-meta">
<span class="feature-title">Ajout de la commande tree pour parcourir une archive</span>
<span class="feature-timeline">08/03 - 14/03</span>
</div>
<div class="feature-owner">Par <strong>Willian Huang Hong</strong></div>
</div>
</div>
</div>

<div class="feature-category">
<div class="category-label">Prétraitements</div>
<div class="sub-feature">
<div class="sub-feature-label">Image</div>
<div class="sub-sub-feature">
<div class="feature-meta">
<span class="feature-title">Intégration du filtre LOCO-I / MED</span>
<span class="feature-timeline">17/02 - 19/02</span>
</div>
<div class="feature-owner">Par <strong>Romain Bailly</strong></div>
</div>
<div class="sub-sub-feature">
<div class="feature-meta">
<span class="feature-title">Ajout de YCoCg-R pour les images</span>
<span class="feature-timeline">13/03 - 17/03</span>
</div>
<div class="feature-owner">Par <strong>Alexandre Joaquim Lima Salgueiro</strong></div>
</div>
</div>
<div class="sub-feature">
<div class="sub-feature-label">Texte</div>
<div class="sub-sub-feature">
<div class="feature-meta">
<span class="feature-title">Ajout de BPE pour les corpus texte</span>
<span class="feature-timeline">15/02 - 17/02</span>
</div>
<div class="feature-owner">Par <strong>Alexandre Joaquim Lima Salgueiro</strong></div>
</div>
<div class="sub-sub-feature">
<div class="feature-meta">
<span class="feature-title">Ajout de BWT pour les corpus texte</span>
<span class="feature-timeline">18/03 - 18/03</span>
</div>
<div class="feature-owner">Par <strong>Alexandre Joaquim Lima Salgueiro</strong></div>
</div>
</div>
<div class="sub-feature">
<div class="sub-feature-label">Binaire et audio</div>
<div class="sub-sub-feature">
<div class="feature-meta">
<span class="feature-title">Transformation BCJ pour les binaires</span>
<span class="feature-timeline">15/02 - 17/02</span>
</div>
<div class="feature-owner">Par <strong>Willian Huang Hong</strong></div>
</div>
</div>
</div>

<div class="feature-category">
<div class="category-label">Documentation</div>
<div class="sub-feature">
<div class="sub-feature-label">Site et contenus</div>
<div class="sub-sub-feature">
<div class="feature-meta">
<span class="feature-title">Création du site de documentation</span>
<span class="feature-timeline">18/03 - 22/03</span>
</div>
<div class="feature-owner">Par <strong>Romain Bailly</strong></div>
</div>
</div>
</div>

<div class="feature-category">
<div class="category-label">Intégrité</div>
<div class="sub-feature">
<div class="sub-feature-label">Vérification des données</div>
<div class="sub-sub-feature">
<div class="feature-meta">
<span class="feature-title">Mise en place des checksums</span>
<span class="feature-timeline">17/03 - 20/03</span>
</div>
<div class="feature-owner">Par <strong>Romain Bailly</strong></div>
</div>
</div>
</div>
</div>
