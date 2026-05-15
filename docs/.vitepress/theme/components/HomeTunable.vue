<template>
  <div class="tunable-section">
    <div class="tunable-container">
      
      <!-- Text Content Side -->
      <div class="tunable-text">
        <div class="principle-label">Notre Principe</div>
        <h2 class="title">
          <span class="gradient-text">Entièrement</span><br>
          Personnalisable
        </h2>
        
        <p class="description">
          Notre objectif est de concevoir un archiveur ergonomique, logique et productif qui vous donne un contrôle total sur l'utilisation de votre matériel.
        </p>
        <p class="description">
          Conçu pour éviter les contraintes rigides, Oxide vous permet d'ajuster le nombre de threads, la capacité des pools et la taille des blocs directement depuis un simple fichier JSON.
        </p>
        
        <p class="highlight-desc">
          Une configuration aussi simple qu'<span class="highlight-text">un fichier JSON</span>
        </p>
      </div>

      <!-- Code Snippet Side -->
      <div class="tunable-code-wrapper">
        <div class="code-window">
          <div class="code-header">
            <span class="filename">crates/oxide-cli/presets.json</span>
          </div>
          <pre class="code-content"><code><span class="keyword">"presets"</span>: {
  <span class="string">"fast"</span>: {
    <span class="keyword">"compression"</span>: {
      <span class="keyword">"compressor"</span>: <span class="string">"lz4"</span>
    },
    <span class="keyword">"block_size"</span>: <span class="string">"3M"</span>,
    <span class="keyword">"workers"</span>: <span class="number">0</span>, <span class="comment">// Détection auto. des cœurs</span>
    <span class="keyword">"pool_capacity"</span>: <span class="string">"3M"</span>,
    <span class="keyword">"pool_buffers"</span>: <span class="number">256</span>,
    <span class="keyword">"stream_read_buffer"</span>: <span class="string">"64M"</span>,
    <span class="keyword">"inflight_bytes"</span>: <span class="string">"2G"</span>
  },
  <span class="string">"balanced"</span>: {
    <span class="keyword">"compression"</span>: {
      <span class="keyword">"compressor"</span>: <span class="string">"zstd"</span>,
      <span class="keyword">"level"</span>: <span class="number">2</span>
    },
    <span class="keyword">"block_size"</span>: <span class="string">"2M"</span>
  }
}</code></pre>
        </div>
      </div>

    </div>

    <!-- Feature Grid below the main section -->
    <div class="features-grid">
      <div class="feature-item">
        <div class="feature-title">
          <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 2L2 7l10 5 10-5-10-5z"></path><path d="M2 17l10 5 10-5"></path><path d="M2 12l10 5 10-5"></path></svg>
          Multi-threading
        </div>
        <div class="feature-desc">Mettez à l'échelle dynamiquement les threads de compression et de production. Détection automatique des cœurs intégrée.</div>
      </div>
      <div class="feature-item">
        <div class="feature-title">
          <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="3" width="18" height="18" rx="2" ry="2"></rect><line x1="9" y1="3" x2="9" y2="21"></line></svg>
          Gestion de la mémoire
        </div>
        <div class="feature-desc">Ajustez finement les pools de buffers et les limites `inflight_bytes` pour éviter les erreurs de mémoire (OOM) sur les données massives.</div>
      </div>
      <div class="feature-item">
        <div class="feature-title">
          <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 2v6h-6"></path><path d="M3 12a9 9 0 1 0 9-9 9.75 9.75 0 0 0-6.74 2.74L3 8"></path><path d="M3 3v6h6"></path></svg>
          Changement d'algorithme
        </div>
        <div class="feature-desc">Basculez de manière transparente entre LZ4, Zstd et LZMA, en vous adaptant parfaitement à votre matériel cible.</div>
      </div>
      <div class="feature-item">
        <div class="feature-title">
          <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="22 12 18 12 15 21 9 3 6 12 2 12"></polyline></svg>
          Optimisation des E/S
        </div>
        <div class="feature-desc">Ajustez les buffers de lecture et la taille des blocs. Adaptez-vous parfaitement aux environnements SSD ou HDD.</div>
      </div>
    </div>
  </div>
</template>

<style scoped>
.tunable-section {
  padding: 0 24px;
  margin-top: 4rem;
  margin-bottom: 5rem;
  display: flex;
  flex-direction: column;
  align-items: center;
}

.tunable-container {
  width: 100%;
  max-width: 1050px;
  display: flex;
  justify-content: space-between;
  align-items: center;
  gap: 4rem;
}

@media (max-width: 900px) {
  .tunable-container {
    flex-direction: column;
    align-items: flex-start;
  }
}

.tunable-text {
  flex: 1;
  max-width: 500px;
}

.principle-label {
  color: #9ca3af;
  font-size: 1.1rem;
  font-weight: 500;
  margin-bottom: 0.5rem;
}

.title {
  font-size: 4rem;
  font-weight: 800;
  line-height: 1.1;
  margin-bottom: 2rem;
  font-family: var(--vp-font-family-base);
  color: var(--vp-c-text-1);
}

.gradient-text {
  background: linear-gradient(120deg, #f472b6, #fb7185);
  -webkit-background-clip: text;
  -webkit-text-fill-color: transparent;
  background-clip: text;
}

.description {
  color: #9ca3af;
  font-size: 1.1rem;
  line-height: 1.6;
  margin-bottom: 1.5rem;
}

.highlight-desc {
  color: #9ca3af;
  font-size: 1.15rem;
  margin-top: 2.5rem;
}

.highlight-text {
  color: #f472b6;
  font-weight: 600;
}

/* Code Snippet styling */
.tunable-code-wrapper {
  flex: 1;
  width: 100%;
  max-width: 500px;
  position: relative;
}

.code-window {
  background: #1e1e24;
  border-radius: 16px;
  overflow: hidden;
  box-shadow: 0 20px 40px rgba(0, 0, 0, 0.3), inset 0 1px 0 rgba(255,255,255,0.1);
  border: 1px solid rgba(255,255,255,0.05);
  position: relative;
}

.code-window::before {
  content: '';
  position: absolute;
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  background: radial-gradient(circle at top left, rgba(244, 114, 182, 0.08), transparent 50%),
              radial-gradient(circle at bottom right, rgba(147, 51, 234, 0.08), transparent 50%);
  pointer-events: none;
}

.code-header {
  padding: 12px 20px;
  background: rgba(0, 0, 0, 0.2);
  border-bottom: 1px solid rgba(255, 255, 255, 0.05);
  font-family: var(--vp-font-family-mono);
  font-size: 0.85rem;
  color: #6b7280;
}

.code-content {
  margin: 0;
  padding: 1.5rem;
  overflow-x: auto;
  font-family: 'JetBrains Mono', 'Fira Code', monospace;
  font-size: 0.95rem;
  line-height: 1.5;
  color: #e2e8f0;
}

.code-content .keyword { color: #f472b6; }
.code-content .string { color: #818cf8; }
.code-content .number { color: #60a5fa; }
.code-content .comment { color: #6b7280; font-style: italic; }

/* Features Grid */
.features-grid {
  display: grid;
  grid-template-columns: repeat(4, 1fr);
  gap: 2rem;
  width: 100%;
  max-width: 1050px;
  margin-top: 4rem;
  border-top: 1px solid rgba(255,255,255,0.05);
  padding-top: 3rem;
}

@media (max-width: 900px) {
  .features-grid {
    grid-template-columns: repeat(2, 1fr);
  }
}

@media (max-width: 600px) {
  .features-grid {
    grid-template-columns: 1fr;
  }
}

.feature-title {
  display: flex;
  align-items: center;
  gap: 10px;
  font-size: 1.1rem;
  font-weight: 600;
  color: #d1d5db;
  margin-bottom: 0.8rem;
}

.feature-title svg {
  color: #9ca3af;
}

.feature-desc {
  font-size: 0.95rem;
  color: #9ca3af;
  line-height: 1.5;
}
</style>