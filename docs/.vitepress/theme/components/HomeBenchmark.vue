<template>
  <div class="benchmark-section">
    
    <div class="benchmark-header">
      <div class="benchmark-tabs">
        <button 
          class="tab-btn" 
          :class="{ active: activeTab === 'archiving' }" 
          @click="activeTab = 'archiving'"
        >
          Archivage
        </button>
        <button 
          class="tab-btn" 
          :class="{ active: activeTab === 'extraction' }" 
          @click="activeTab = 'extraction'"
        >
          Extraction
        </button>
      </div>
    </div>

    <!-- ARCHIVING TAB -->
    <div v-show="activeTab === 'archiving'" class="benchmark-container" :class="{ 'is-visible': isVisible }">
      <div class="benchmark-grid"></div>
      <div class="benchmark-content">
        
        <div class="benchmark-stats">
          <div class="stat-block">
            <div class="stat-multiplier primary-gradient">1.2x</div>
            <div class="stat-desc">plus rapide que<br>squashfs<br><span style="font-size: 0.85em; opacity: 0.7">(Mode Rapide)</span></div>
          </div>
          <div class="stat-block">
            <div class="stat-multiplier secondary-gradient">2.4x</div>
            <div class="stat-desc">plus rapide que<br>7zip<br><span style="font-size: 0.85em; opacity: 0.7">(Mode Ultra)</span></div>
          </div>
        </div>
        
        <div class="benchmark-bars">
          <!-- Fast Mode -->
          <div class="mode-title">Mode Rapide</div>
          <div class="bar-row">
            <div class="bar-label"><span class="brand-text">Oxide</span></div>
            <div class="bar-track-container">
              <div class="bar-fill oxide-bar" style="--target-width: 100%;">
                <span class="bar-value">1 463 MiB/s</span>
              </div>
            </div>
          </div>
          <div class="bar-row">
            <div class="bar-label">squashfs</div>
            <div class="bar-track-container">
              <div class="bar-fill" style="--target-width: 81%;">
                <span class="bar-value">1 187 MiB/s</span>
              </div>
            </div>
          </div>
          <div class="bar-row">
            <div class="bar-label">tar+zstd</div>
            <div class="bar-track-container">
              <div class="bar-fill" style="--target-width: 77%;">
                <span class="bar-value">1 131 MiB/s</span>
              </div>
            </div>
          </div>
          
          <div class="bar-row spacer"></div>

          <!-- Balanced Mode -->
          <div class="mode-title">Mode Équilibré</div>
          <div class="bar-row">
            <div class="bar-label"><span class="brand-text">Oxide</span></div>
            <div class="bar-track-container">
              <div class="bar-fill oxide-bar-dim" style="--target-width: 100%;">
                <span class="bar-value">1 305 MiB/s</span>
              </div>
            </div>
          </div>
          <div class="bar-row">
            <div class="bar-label">squashfs</div>
            <div class="bar-track-container">
              <div class="bar-fill" style="--target-width: 93%;">
                <span class="bar-value">1 215 MiB/s</span>
              </div>
            </div>
          </div>
          <div class="bar-row">
            <div class="bar-label">tar+zstd</div>
            <div class="bar-track-container">
              <div class="bar-fill" style="--target-width: 86%;">
                <span class="bar-value">1 118 MiB/s</span>
              </div>
            </div>
          </div>

          <div class="bar-row spacer"></div>

          <!-- Ultra Mode -->
          <div class="mode-title">Mode Ultra</div>
          <div class="bar-row">
            <div class="bar-label"><span class="brand-text">Oxide</span></div>
            <div class="bar-track-container">
              <div class="bar-fill oxide-bar-dark" style="--target-width: 100%;">
                <span class="bar-value">156 MiB/s</span>
              </div>
            </div>
          </div>
          <div class="bar-row">
            <div class="bar-label">dwarfs</div>
            <div class="bar-track-container">
              <div class="bar-fill" style="--target-width: 42%;">
                <span class="bar-value">66 MiB/s</span>
              </div>
            </div>
          </div>
          <div class="bar-row">
            <div class="bar-label">7zip</div>
            <div class="bar-track-container">
              <div class="bar-fill" style="--target-width: 42%;">
                <span class="bar-value">65 MiB/s</span>
              </div>
            </div>
          </div>
          <div class="bar-row">
            <div class="bar-label">pixz</div>
            <div class="bar-track-container">
              <div class="bar-fill" style="--target-width: 19%;">
                <span class="bar-value">30 MiB/s</span>
              </div>
            </div>
          </div>

          <div class="benchmark-note">
            Débit mesuré en MiB/s.
          </div>
        </div>
      </div>
    </div>

    <!-- EXTRACTION TAB -->
    <div v-show="activeTab === 'extraction'" class="benchmark-container" :class="{ 'is-visible': isVisible }">
      <div class="benchmark-grid"></div>
      <div class="benchmark-content">
        
        <div class="benchmark-stats">
          <div class="stat-block">
            <div class="stat-multiplier primary-gradient">1.1x</div>
            <div class="stat-desc">plus rapide que<br>squashfs<br><span style="font-size: 0.85em; opacity: 0.7">(Mode Rapide)</span></div>
          </div>
          <div class="stat-block">
            <div class="stat-multiplier secondary-gradient">3.4x</div>
            <div class="stat-desc">plus rapide que<br>7zip<br><span style="font-size: 0.85em; opacity: 0.7">(Mode Ultra)</span></div>
          </div>
        </div>
        
        <div class="benchmark-bars">
          <!-- Fast Mode -->
          <div class="mode-title">Mode Rapide</div>
          <div class="bar-row">
            <div class="bar-label"><span class="brand-text">Oxide</span></div>
            <div class="bar-track-container">
              <div class="bar-fill oxide-bar" style="--target-width: 100%;">
                <span class="bar-value">1 956 MiB/s</span>
              </div>
            </div>
          </div>
          <div class="bar-row">
            <div class="bar-label">squashfs</div>
            <div class="bar-track-container">
              <div class="bar-fill" style="--target-width: 88%;">
                <span class="bar-value">1 714 MiB/s</span>
              </div>
            </div>
          </div>
          <div class="bar-row">
            <div class="bar-label">tar+zstd</div>
            <div class="bar-track-container">
              <div class="bar-fill" style="--target-width: 72%;">
                <span class="bar-value">1 402 MiB/s</span>
              </div>
            </div>
          </div>
          
          <div class="bar-row spacer"></div>

          <!-- Balanced Mode -->
          <div class="mode-title">Mode Équilibré</div>
          <div class="bar-row">
            <div class="bar-label"><span class="brand-text">Oxide</span></div>
            <div class="bar-track-container">
              <div class="bar-fill oxide-bar-dim" style="--target-width: 100%;">
                <span class="bar-value">2 025 MiB/s</span>
              </div>
            </div>
          </div>
          <div class="bar-row">
            <div class="bar-label">squashfs</div>
            <div class="bar-track-container">
              <div class="bar-fill" style="--target-width: 89%;">
                <span class="bar-value">1 803 MiB/s</span>
              </div>
            </div>
          </div>
          <div class="bar-row">
            <div class="bar-label">tar+zstd</div>
            <div class="bar-track-container">
              <div class="bar-fill" style="--target-width: 69%;">
                <span class="bar-value">1 388 MiB/s</span>
              </div>
            </div>
          </div>

          <div class="bar-row spacer"></div>

          <!-- Ultra Mode -->
          <div class="mode-title">Mode Ultra</div>
          <div class="bar-row">
            <div class="bar-label"><span class="brand-text">Oxide</span></div>
            <div class="bar-track-container">
              <div class="bar-fill oxide-bar-dark" style="--target-width: 100%;">
                <span class="bar-value">1 831 MiB/s</span>
              </div>
            </div>
          </div>
          <div class="bar-row">
            <div class="bar-label">dwarfs</div>
            <div class="bar-track-container">
              <div class="bar-fill" style="--target-width: 76%;">
                <span class="bar-value">1 395 MiB/s</span>
              </div>
            </div>
          </div>
          <div class="bar-row">
            <div class="bar-label">pixz</div>
            <div class="bar-track-container">
              <div class="bar-fill" style="--target-width: 44%;">
                <span class="bar-value">797 MiB/s</span>
              </div>
            </div>
          </div>
          <div class="bar-row">
            <div class="bar-label">7zip</div>
            <div class="bar-track-container">
              <div class="bar-fill" style="--target-width: 30%;">
                <span class="bar-value">546 MiB/s</span>
              </div>
            </div>
          </div>

          <div class="benchmark-note">
            Débit mesuré en MiB/s.
          </div>
        </div>
      </div>
    </div>

  </div>
</template>

<script setup>
import { ref, onMounted } from 'vue'

const activeTab = ref('archiving')
const isVisible = ref(false)

onMounted(() => {
  // Simple delay to trigger the animation on load if no observer
  setTimeout(() => {
    isVisible.value = true
  }, 100)
})
</script>

<style scoped>
.benchmark-section {
  padding: 0 24px;
  margin-top: -1rem;
  margin-bottom: 5rem;
  display: flex;
  flex-direction: column;
  align-items: center;
}

.benchmark-header {
  margin-bottom: 1.5rem;
  display: flex;
  justify-content: center;
  width: 100%;
}

.benchmark-tabs {
  display: inline-flex;
  background: rgba(0, 0, 0, 0.2);
  padding: 4px;
  border-radius: 40px;
  border: 1px solid rgba(255, 255, 255, 0.05);
}

.dark .benchmark-tabs {
  background: #000000;
}

.tab-btn {
  padding: 8px 20px;
  border-radius: 30px;
  border: none;
  background: transparent;
  color: #9ca3af;
  font-weight: 600;
  font-size: 0.95rem;
  cursor: pointer;
  transition: all 0.3s ease;
}

.tab-btn:hover {
  color: #f3ece6;
}

.tab-btn.active {
  background: #374151;
  color: #ffffff;
  box-shadow: 0 2px 10px rgba(0, 0, 0, 0.2);
}

.dark .tab-btn.active {
  background: #2d2623;
  color: #fb923c;
}

.benchmark-container {
  width: 100%;
  max-width: 1000px;
  background-color: #1a1820;
  border-radius: 16px;
  position: relative;
  overflow: hidden;
  padding: 3rem 4rem;
  box-shadow: 0 20px 40px rgba(0, 0, 0, 0.15);
  border: 1px solid rgba(255, 255, 255, 0.05);
  display: flex;
  flex-direction: column;
}

.dark .benchmark-container {
  background-color: #16141d;
  box-shadow: 0 20px 40px rgba(0, 0, 0, 0.4);
}

.benchmark-grid {
  position: absolute;
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  background-image: 
    linear-gradient(rgba(255, 255, 255, 0.03) 1px, transparent 1px),
    linear-gradient(90deg, rgba(255, 255, 255, 0.03) 1px, transparent 1px);
  background-size: 40px 40px;
  z-index: 0;
  pointer-events: none;
}

.benchmark-content {
  position: relative;
  z-index: 1;
  display: flex;
  gap: 5rem;
  align-items: stretch;
}

@media (max-width: 820px) {
  .benchmark-content {
    flex-direction: column;
    gap: 3rem;
  }
  .benchmark-container {
    padding: 2.5rem 2rem;
  }
}

.benchmark-stats {
  display: flex;
  flex-direction: column;
  justify-content: center;
  gap: 3rem;
  min-width: 180px;
}

.stat-block {
  display: flex;
  flex-direction: column;
}

.stat-multiplier {
  font-size: 5rem;
  font-weight: 800;
  line-height: 1;
  margin-bottom: 0.5rem;
  letter-spacing: -3px;
  font-family: var(--vp-font-family-base);
}

.primary-gradient {
  background: linear-gradient(120deg, #f472b6, #ec4899);
  -webkit-background-clip: text;
  -webkit-text-fill-color: transparent;
  background-clip: text;
}

.secondary-gradient {
  background: linear-gradient(120deg, #d8b4fe, #c084fc);
  -webkit-background-clip: text;
  -webkit-text-fill-color: transparent;
  background-clip: text;
}

.stat-desc {
  color: #9ca3af;
  font-size: 1.1rem;
  line-height: 1.3;
  font-weight: 400;
}

.benchmark-bars {
  flex: 1;
  display: flex;
  flex-direction: column;
  gap: 0.7rem;
  width: 100%;
}

.mode-title {
  font-size: 0.8rem;
  text-transform: uppercase;
  letter-spacing: 0.1em;
  color: #6b7280;
  font-weight: 700;
  margin-top: 0.5rem;
  margin-bottom: 0.2rem;
}

.bar-row {
  display: flex;
  align-items: center;
  gap: 1.5rem;
  height: 24px;
}

.bar-row.spacer {
  height: 8px;
}

.bar-label {
  width: 120px;
  text-align: right;
  font-family: var(--vp-font-family-mono);
  font-size: 0.85rem;
  color: #d1d5db;
  white-space: nowrap;
}

.bar-label .brand-text {
  color: #60a5fa;
  font-weight: 700;
}

.bar-track-container {
  flex: 1;
  height: 100%;
  position: relative;
  display: flex;
  align-items: center;
}

.bar-fill {
  height: 100%;
  background-color: #374151; /* Dark grey */
  border-radius: 4px;
  position: relative;
  width: 0;
  transition: width 1.2s cubic-bezier(0.22, 1, 0.36, 1);
}

.bar-value {
  position: absolute;
  left: calc(100% + 8px);
  top: 50%;
  transform: translateY(-50%);
  font-family: var(--vp-font-family-mono);
  font-size: 0.8rem;
  color: #9ca3af;
  opacity: 0;
  white-space: nowrap;
  transition: opacity 0.4s ease 0.5s;
}

.oxide-bar {
  background: linear-gradient(90deg, #a855f7, #ec4899);
}

.oxide-bar-dim {
  background: linear-gradient(90deg, #9333ea, #db2777);
}

.oxide-bar-dark {
  background: linear-gradient(90deg, #7e22ce, #be185d);
}

.oxide-bar .bar-value, .oxide-bar-dim .bar-value, .oxide-bar-dark .bar-value {
  left: auto;
  right: 8px;
  color: #ffffff;
  font-weight: 600;
}

.benchmark-container.is-visible .bar-fill {
  width: var(--target-width);
}

.benchmark-container.is-visible .bar-value {
  opacity: 1;
}

.benchmark-note {
  margin-top: 1.5rem;
  font-size: 0.8rem;
  color: #6b7280;
  text-align: left;
  border-top: 1px solid rgba(255,255,255,0.05);
  padding-top: 1rem;
}
</style>