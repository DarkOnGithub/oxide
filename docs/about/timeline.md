---
title: Chronologie du projet
description: Frise indicative des étapes majeures du développement d'Oxide
---

# Chronologie du projet

> Les dates ci-dessous sont indicatives et reconstituent une progression cohérente du projet entre février et mai 2026.

<div class="oxide-rail">
  <div class="oxide-rail-phases">
    <section class="oxide-rail-phase">
      <div class="oxide-rail-meta">
        <span class="oxide-rail-id">01</span>
        <h2>Fondations du moteur</h2>
      </div>
      <div class="oxide-rail-events">
        <article class="oxide-rail-event">
          <div class="oxide-rail-event-body">
            <div class="oxide-rail-event-head">
              <span class="oxide-rail-tag">Architecture</span>
              <time class="oxide-rail-date">03/02 - 07/02</time>
            </div>
            <h3>Mise en place du workspace Rust et des crates principales</h3>
            <p class="oxide-rail-owner">Équipe complète</p>
          </div>
        </article>
        <article class="oxide-rail-event">
          <div class="oxide-rail-event-body">
            <div class="oxide-rail-event-head">
              <span class="oxide-rail-tag">Compression</span>
              <time class="oxide-rail-date">08/02 - 14/02</time>
            </div>
            <h3>Premier pipeline parallèle et intégration de LZ4</h3>
            <p class="oxide-rail-owner">Tom Huynh</p>
          </div>
        </article>
        <article class="oxide-rail-event">
          <div class="oxide-rail-event-body">
            <div class="oxide-rail-event-head">
              <span class="oxide-rail-tag">Format</span>
              <time class="oxide-rail-date">10/02 - 18/02</time>
            </div>
            <h3>Première structuration du format `.oxz` et du manifest</h3>
            <p class="oxide-rail-owner">Tom Huynh</p>
          </div>
        </article>
        <article class="oxide-rail-event">
          <div class="oxide-rail-event-body">
            <div class="oxide-rail-event-head">
              <span class="oxide-rail-tag">CLI</span>
              <time class="oxide-rail-date">12/02 - 20/02</time>
            </div>
            <h3>Premières commandes `archive` et `extract`</h3>
            <p class="oxide-rail-owner">William Huang Hong</p>
          </div>
        </article>
      </div>
    </section>
    <section class="oxide-rail-phase">
      <div class="oxide-rail-meta">
        <span class="oxide-rail-id">02</span>
        <h2>Compression avancée</h2>
      </div>
      <div class="oxide-rail-events">
        <article class="oxide-rail-event">
          <div class="oxide-rail-event-body">
            <div class="oxide-rail-event-head">
              <span class="oxide-rail-tag">Preset</span>
              <time class="oxide-rail-date">24/02 - 28/02</time>
            </div>
            <h3>Stabilisation des profils `fast`, `balanced` et `ultra`</h3>
            <p class="oxide-rail-owner">Tom Huynh</p>
          </div>
        </article>
        <article class="oxide-rail-event">
          <div class="oxide-rail-event-body">
            <div class="oxide-rail-event-head">
              <span class="oxide-rail-tag">Compression</span>
              <time class="oxide-rail-date">01/03 - 07/03</time>
            </div>
            <h3>Remplacement du mode équilibré initial par Zstandard</h3>
            <p class="oxide-rail-owner">Tom Huynh</p>
          </div>
        </article>
        <article class="oxide-rail-event">
          <div class="oxide-rail-event-body">
            <div class="oxide-rail-event-head">
              <span class="oxide-rail-tag">Compression</span>
              <time class="oxide-rail-date">08/03 - 16/03</time>
            </div>
            <h3>Intégration de LZMA pour le mode ultra</h3>
            <p class="oxide-rail-owner">Tom Huynh</p>
          </div>
        </article>
        <article class="oxide-rail-event">
          <div class="oxide-rail-event-body">
            <div class="oxide-rail-event-head">
              <span class="oxide-rail-tag">Pipeline</span>
              <time class="oxide-rail-date">10/03 - 20/03</time>
            </div>
            <h3>Ajout du stockage brut automatique, du backpressure et des buffers réutilisables</h3>
            <p class="oxide-rail-owner">Tom Huynh</p>
          </div>
        </article>
      </div>
    </section>
    <section class="oxide-rail-phase">
      <div class="oxide-rail-meta">
        <span class="oxide-rail-id">03</span>
        <h2>Sécurité et résilience</h2>
      </div>
      <div class="oxide-rail-events">
        <article class="oxide-rail-event">
          <div class="oxide-rail-event-body">
            <div class="oxide-rail-event-head">
              <span class="oxide-rail-tag">Sécurité</span>
              <time class="oxide-rail-date">12/03 - 19/03</time>
            </div>
            <h3>Dérivation de clé Argon2id et chiffrement AES-256-GCM</h3>
            <p class="oxide-rail-owner">Alexandre Joaquim Lima Salgueiro</p>
          </div>
        </article>
        <article class="oxide-rail-event">
          <div class="oxide-rail-event-body">
            <div class="oxide-rail-event-head">
              <span class="oxide-rail-tag">Intégrité</span>
              <time class="oxide-rail-date">18/03 - 24/03</time>
            </div>
            <h3>Détection locale des corruptions par CRC32C</h3>
            <p class="oxide-rail-owner">William Huang Hong</p>
          </div>
        </article>
        <article class="oxide-rail-event">
          <div class="oxide-rail-event-body">
            <div class="oxide-rail-event-head">
              <span class="oxide-rail-tag">Réparation</span>
              <time class="oxide-rail-date">22/03 - 31/03</time>
            </div>
            <h3>Architecture de réparation Out-of-Band et moteur Reed-Solomon</h3>
            <p class="oxide-rail-owner">William Huang Hong &amp; Alexandre Salgueiro</p>
          </div>
        </article>
        <article class="oxide-rail-event">
          <div class="oxide-rail-event-body">
            <div class="oxide-rail-event-head">
              <span class="oxide-rail-tag">CLI</span>
              <time class="oxide-rail-date">29/03 - 03/04</time>
            </div>
            <h3>Ajout des commandes `protect` et `repair`</h3>
            <p class="oxide-rail-owner">Alexandre Joaquim Lima Salgueiro</p>
          </div>
        </article>
      </div>
    </section>
    <section class="oxide-rail-phase">
      <div class="oxide-rail-meta">
        <span class="oxide-rail-id">04</span>
        <h2>Interface graphique</h2>
      </div>
      <div class="oxide-rail-events">
        <article class="oxide-rail-event">
          <div class="oxide-rail-event-body">
            <div class="oxide-rail-event-head">
              <span class="oxide-rail-tag">GUI</span>
              <time class="oxide-rail-date">01/04 - 10/04</time>
            </div>
            <h3>Construction d'Oxide Toolkit avec `egui` / `eframe`</h3>
            <p class="oxide-rail-owner">Romain Bailly</p>
          </div>
        </article>
        <article class="oxide-rail-event">
          <div class="oxide-rail-event-body">
            <div class="oxide-rail-event-head">
              <span class="oxide-rail-tag">UX</span>
              <time class="oxide-rail-date">08/04 - 16/04</time>
            </div>
            <h3>Intégration du chiffrement, de la protection et de la double saisie mot de passe</h3>
            <p class="oxide-rail-owner">Romain Bailly</p>
          </div>
        </article>
        <article class="oxide-rail-event">
          <div class="oxide-rail-event-body">
            <div class="oxide-rail-event-head">
              <span class="oxide-rail-tag">Stabilité</span>
              <time class="oxide-rail-date">14/04 - 21/04</time>
            </div>
            <h3>Correction des blocages Wayland et limitation des rafraîchissements de progression</h3>
            <p class="oxide-rail-owner">Romain Bailly</p>
          </div>
        </article>
      </div>
    </section>
    <section class="oxide-rail-phase">
      <div class="oxide-rail-meta">
        <span class="oxide-rail-id">05</span>
        <h2>Validation et soutenance</h2>
      </div>
      <div class="oxide-rail-events">
        <article class="oxide-rail-event">
          <div class="oxide-rail-event-body">
            <div class="oxide-rail-event-head">
              <span class="oxide-rail-tag">Benchmarks</span>
              <time class="oxide-rail-date">22/04 - 02/05</time>
            </div>
            <h3>Campagne comparative sur un jeu de données mixte de 5.7 GB</h3>
            <p class="oxide-rail-owner">Tom Huynh</p>
          </div>
        </article>
        <article class="oxide-rail-event">
          <div class="oxide-rail-event-body">
            <div class="oxide-rail-event-head">
              <span class="oxide-rail-tag">Documentation</span>
              <time class="oxide-rail-date">04/05 - 12/05</time>
            </div>
            <h3>Mise à jour du site, des pages À propos et des supports de démonstration</h3>
            <p class="oxide-rail-owner">Romain Bailly</p>
          </div>
        </article>
        <article class="oxide-rail-event">
          <div class="oxide-rail-event-body">
            <div class="oxide-rail-event-head">
              <span class="oxide-rail-tag">Livrable</span>
              <time class="oxide-rail-date">18/05</time>
            </div>
            <h3>Deuxième soutenance : moteur stabilisé, GUI démontrable et rapport technique consolidé</h3>
            <p class="oxide-rail-owner">Équipe complète</p>
          </div>
        </article>
      </div>
    </section>
  </div>
</div>
