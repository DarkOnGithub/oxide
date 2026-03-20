---
title: Chronologie du projet
description: Frise indicative du développement d'Oxide à partir des contributions réellement visibles dans le dépôt
---

# Chronologie du projet

<div class="oxide-rail">
  <div class="oxide-rail-phases">
    <section class="oxide-rail-phase">
      <div class="oxide-rail-meta">
        <span class="oxide-rail-id">01</span>
        <h2>Compression et format</h2>
      </div>
      <div class="oxide-rail-events">
        <article class="oxide-rail-event">
          <div class="oxide-rail-event-body">
            <div class="oxide-rail-event-head">
              <span class="oxide-rail-tag">Compression</span>
              <time class="oxide-rail-date">10/02 - 12/02</time>
            </div>
            <h3>Compression rapide avec LZ4</h3>
            <p class="oxide-rail-owner">Tom Huynh</p>
          </div>
        </article>
        <article class="oxide-rail-event">
          <div class="oxide-rail-event-body">
            <div class="oxide-rail-event-head">
              <span class="oxide-rail-tag">Format</span>
              <time class="oxide-rail-date">À partir du 06/02</time>
            </div>
            <h3>Implémentation du format OXZ</h3>
            <p class="oxide-rail-owner">Tom Huynh</p>
          </div>
        </article>
        <article class="oxide-rail-event">
          <div class="oxide-rail-event-body">
            <div class="oxide-rail-event-head">
              <span class="oxide-rail-tag">Format et pipeline</span>
              <time class="oxide-rail-date">À partir du 06/02</time>
            </div>
            <h3>Mise en place du pipeline d'archivage</h3>
            <p class="oxide-rail-owner">Tom Huynh</p>
          </div>
        </article>
        <article class="oxide-rail-event">
          <div class="oxide-rail-event-body">
            <div class="oxide-rail-event-head">
              <span class="oxide-rail-tag">Exécution</span>
              <time class="oxide-rail-date">06/02 - 11/02</time>
            </div>
            <h3>Travail sur l'exécution parallèle et le work stealing</h3>
            <p class="oxide-rail-owner">Tom Huynh</p>
          </div>
        </article>
        <article class="oxide-rail-event">
          <div class="oxide-rail-event-body">
            <div class="oxide-rail-event-head">
              <span class="oxide-rail-tag">Observabilité</span>
              <time class="oxide-rail-date">15/02 - 10/03</time>
            </div>
            <h3>Système de télémétrie et rapports de performance</h3>
            <p class="oxide-rail-owner">Tom Huynh</p>
          </div>
        </article>
        <article class="oxide-rail-event">
          <div class="oxide-rail-event-body">
            <div class="oxide-rail-event-head">
              <span class="oxide-rail-tag">Mémoire</span>
              <time class="oxide-rail-date">06/02 - 16/03</time>
            </div>
            <h3>Mise en place des buffers réutilisables et des lectures en mémoire</h3>
            <p class="oxide-rail-owner">Tom Huynh</p>
          </div>
        </article>
        <article class="oxide-rail-event">
          <div class="oxide-rail-event-body">
            <div class="oxide-rail-event-head">
              <span class="oxide-rail-tag">Format</span>
              <time class="oxide-rail-date">06/02 - 14/03</time>
            </div>
            <h3>Organisation du scan, du manifest et de la lecture des archives</h3>
            <p class="oxide-rail-owner">Tom Huynh</p>
          </div>
        </article>
      </div>
    </section>
    <section class="oxide-rail-phase">
      <div class="oxide-rail-meta">
        <span class="oxide-rail-id">02</span>
        <h2>Interface et usages</h2>
      </div>
      <div class="oxide-rail-events">
        <article class="oxide-rail-event">
          <div class="oxide-rail-event-body">
            <div class="oxide-rail-event-head">
              <span class="oxide-rail-tag">Commandes CLI</span>
              <time class="oxide-rail-date">06/02 - 09/02</time>
            </div>
            <h3>Ajout de la commande archive</h3>
            <p class="oxide-rail-owner">Willian Huang Hong</p>
          </div>
        </article>
        <article class="oxide-rail-event">
          <div class="oxide-rail-event-body">
            <div class="oxide-rail-event-head">
              <span class="oxide-rail-tag">Commandes CLI</span>
              <time class="oxide-rail-date">09/02 - 12/02</time>
            </div>
            <h3>Ajout de la commande extract</h3>
            <p class="oxide-rail-owner">Willian Huang Hong</p>
          </div>
        </article>
        <article class="oxide-rail-event">
          <div class="oxide-rail-event-body">
            <div class="oxide-rail-event-head">
              <span class="oxide-rail-tag">Configuration</span>
              <time class="oxide-rail-date">20/02 - 25/02</time>
            </div>
            <h3>Système de presets et profils d'archivage</h3>
            <p class="oxide-rail-owner">Willian Huang Hong</p>
          </div>
        </article>
        <article class="oxide-rail-event">
          <div class="oxide-rail-event-body">
            <div class="oxide-rail-event-head">
              <span class="oxide-rail-tag">Commandes CLI</span>
              <time class="oxide-rail-date">08/03 - 14/03</time>
            </div>
            <h3>Ajout de la commande tree pour parcourir une archive</h3>
            <p class="oxide-rail-owner">Willian Huang Hong</p>
          </div>
        </article>
      </div>
    </section>
    <section class="oxide-rail-phase">
      <div class="oxide-rail-meta">
        <span class="oxide-rail-id">03</span>
        <h2>Prétraitements</h2>
      </div>
      <div class="oxide-rail-events">
        <article class="oxide-rail-event">
          <div class="oxide-rail-event-body">
            <div class="oxide-rail-event-head">
              <span class="oxide-rail-tag">Image</span>
              <time class="oxide-rail-date">17/02 - 19/02</time>
            </div>
            <h3>Intégration du filtre LOCO-I / MED</h3>
            <p class="oxide-rail-owner">Romain Bailly</p>
          </div>
        </article>
        <article class="oxide-rail-event">
          <div class="oxide-rail-event-body">
            <div class="oxide-rail-event-head">
              <span class="oxide-rail-tag">Image</span>
              <time class="oxide-rail-date">13/03 - 17/03</time>
            </div>
            <h3>Ajout de YCoCg-R pour les images</h3>
            <p class="oxide-rail-owner">Alexandre Joaquim Lima Salgueiro</p>
          </div>
        </article>
        <article class="oxide-rail-event">
          <div class="oxide-rail-event-body">
            <div class="oxide-rail-event-head">
              <span class="oxide-rail-tag">Texte</span>
              <time class="oxide-rail-date">15/02 - 17/02</time>
            </div>
            <h3>Ajout de BPE pour les corpus texte</h3>
            <p class="oxide-rail-owner">Alexandre Joaquim Lima Salgueiro</p>
          </div>
        </article>
        <article class="oxide-rail-event">
          <div class="oxide-rail-event-body">
            <div class="oxide-rail-event-head">
              <span class="oxide-rail-tag">Texte</span>
              <time class="oxide-rail-date">18/03 - 18/03</time>
            </div>
            <h3>Ajout de BWT pour les corpus texte</h3>
            <p class="oxide-rail-owner">Alexandre Joaquim Lima Salgueiro</p>
          </div>
        </article>
        <article class="oxide-rail-event">
          <div class="oxide-rail-event-body">
            <div class="oxide-rail-event-head">
              <span class="oxide-rail-tag">Binaire</span>
              <time class="oxide-rail-date">15/02 - 17/02</time>
            </div>
            <h3>Transformation BCJ pour les binaires</h3>
            <p class="oxide-rail-owner">Willian Huang Hong</p>
          </div>
        </article>
      </div>
    </section>
    <section class="oxide-rail-phase">
      <div class="oxide-rail-meta">
        <span class="oxide-rail-id">04</span>
        <h2>Documentation</h2>
      </div>
      <div class="oxide-rail-events">
        <article class="oxide-rail-event">
          <div class="oxide-rail-event-body">
            <div class="oxide-rail-event-head">
              <span class="oxide-rail-tag">Site et contenus</span>
              <time class="oxide-rail-date">18/03 - 22/03</time>
            </div>
            <h3>Création du site de documentation</h3>
            <p class="oxide-rail-owner">Romain Bailly</p>
          </div>
        </article>
      </div>
    </section>
    <section class="oxide-rail-phase">
      <div class="oxide-rail-meta">
        <span class="oxide-rail-id">05</span>
        <h2>Intégrité</h2>
      </div>
      <div class="oxide-rail-events">
        <article class="oxide-rail-event">
          <div class="oxide-rail-event-body">
            <div class="oxide-rail-event-head">
              <span class="oxide-rail-tag">Vérification des données</span>
              <time class="oxide-rail-date">17/03 - 20/03</time>
            </div>
            <h3>Mise en place des checksums</h3>
            <p class="oxide-rail-owner">Romain Bailly</p>
          </div>
        </article>
      </div>
    </section>
  </div>
</div>
