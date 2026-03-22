<template>
  <div class="hero">
    <div class="glow-bg">
      <div class="orb orb-1"></div>
      <div class="orb orb-2"></div>
      <div class="orb orb-3"></div>
      <div class="orb orb-4"></div>
    </div>
    <div class="background-orbits">
      <div class="orbit orbit-1"></div>
      <div class="orbit orbit-2"></div>
    </div>
    
    <div class="hero-content">
      <div class="logo-container">
        <!-- Slightly animated image logo -->
        <img :src="withBase('/logo.png')" alt="Oxide Logo" class="logo-image animated-logo" />
      </div>
      
      <h2 class="oxide-title text-gradient">Oxide</h2>
      <p class="subtitle">
        Outil d'archivage haute performance en Rust
      </p>
      
      <div class="actions">
        <a :href="withBase('/cli/')" class="btn primary">Prise en main</a>
        <a href="https://github.com/DarkOnGithub/oxide/releases/latest" target="_blank" rel="noopener noreferrer" class="github-download elysia-style" title="Download latest release from GitHub">
          <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"></path><polyline points="7 10 12 15 17 10"></polyline><line x1="12" y1="15" x2="12" y2="3"></line></svg>
        </a>
      </div>
      


    </div>
  </div>
</template>

<script setup>
import { withBase } from 'vitepress'
</script>

<style scoped>
.hero {
  height: calc(100vh - var(--vp-nav-height));
  padding: 0 24px;
  display: flex;
  flex-direction: column;
  justify-content: center;
  align-items: center;
  text-align: center;
  position: relative;
  z-index: 1; /* Above glowing orbs */
  overflow: hidden;
}

.background-orbits {
  position: absolute;
  top: 50%; /* Centers relative to the viewport minimum to prevent top cropping */
  left: 50%;
  transform: translate(-50%, -50%);
  width: 100%; height: 100%;
  pointer-events: none;
  z-index: -1;
  display: flex;
  justify-content: center;
  align-items: center;
}

.orbit {
  position: absolute;
  border-radius: 50%;
  border: 1px solid rgba(0, 0, 0, 0.15); /* Light mode */
  animation: spin-orbit linear infinite;
}
.dark .orbit {
  border-color: rgba(255, 255, 255, 0.05); /* slightly increased from 0.04 */
}

/* Hollow tracking circles on the orbits styling like the reference image */
.orbit::before {
  content: '';
  position: absolute;
  top: -12px; /* Center perfectly on the orbit border */
  left: 50%;
  transform: translateX(-50%);
  width: 24px; height: 24px;
  background-color: var(--vp-c-bg); /* Match background to appear hollow */
  border: 1px solid rgba(0, 0, 0, 0.25); /* Light mode */
  border-radius: 50%;
}
.dark .orbit::before {
  border-color: rgba(255, 255, 255, 0.15); /* Distinct ring outline */
}

.orbit-1 { width: 90vmin; height: 90vmin; animation-duration: 90s; }
.orbit-2 { width: 105vmin; height: 105vmin; animation-duration: 150s; animation-direction: reverse; }

@keyframes spin-orbit {
  0% { transform: rotate(0deg); }
  100% { transform: rotate(360deg); }
}

.hero-content {
  width: 100%;
  max-width: 1100px;
  display: flex;
  flex-direction: column;
  align-items: center;
}

.logo-container {
  margin-bottom: -50px; /* Pulls the text below it significantly closer */
  position: relative;
  z-index: 2;
}
.logo-image {
  max-width: 400px;
  height: auto;
  filter: drop-shadow(0 0 40px rgba(161, 196, 253, 0.15));
}

.oxide-title {
  font-size: 5rem;
  font-weight: 900;
  margin-bottom: 12px;
  line-height: 1.2;
  padding-bottom: 10px; /* Safely clears any gradient-clip clipping from the bottom */
  letter-spacing: -2px;
  background: linear-gradient(120deg, #111827 0%, #4b5563 100%); /* Light mode colors */
  -webkit-background-clip: text;
  -webkit-text-fill-color: transparent;
  background-clip: text;
}
.dark .oxide-title {
  background: linear-gradient(120deg, #ffffff 0%, #d1d5db 100%);
  -webkit-background-clip: text;
  -webkit-text-fill-color: transparent;
  background-clip: text;
}

.subtitle {
  font-size: 1.5rem;
  font-weight: 400;
  color: var(--vp-c-text-2);
  margin-bottom: 30px;
  max-width: 650px;
  line-height: 1.6;
}

.actions {
  display: flex;
  gap: 20px;
  align-items: center;
  justify-content: center;
  flex-wrap: wrap;
  margin-bottom: 0px;
}

.btn {
  display: inline-block;
  padding: 14px 28px;
  border-radius: 40px;
  font-weight: 600;
  font-size: 1.1rem;
  text-decoration: none;
  transition: all 0.3s ease;
}

.btn.primary {
  background-color: #f97316; /* Stronger orange for contrast */
  color: white;
  box-shadow: 0 4px 20px rgba(249, 115, 22, 0.4);
}
.btn.primary:hover {
  background-color: #ea580c; /* Deep rich orange on hover */
  box-shadow: 0 8px 30px rgba(234, 88, 12, 0.6);
  transform: translateY(-2px);
}

.dark .btn.primary {
  background-color: var(--vp-c-brand-1); /* Lighter orange for dark mode */
  box-shadow: 0 4px 20px rgba(251, 146, 60, 0.4);
}
.dark .btn.primary:hover {
  background-color: var(--vp-c-brand-2); /* Even lighter on hover */
  box-shadow: 0 8px 30px rgba(253, 186, 116, 0.6);
  transform: translateY(-2px);
}

.code-copy.elysia-style {
  display: flex;
  align-items: center;
  background-color: var(--vp-c-bg-mute); /* Light mode */
  border: 1px solid var(--vp-c-divider);
  padding: 12px 24px;
  border-radius: 40px;
  gap: 16px;
  cursor: pointer;
  transition: all 0.2s;
}
.dark .code-copy.elysia-style {
  background-color: #050505;
  border-color: rgba(255, 255, 255, 0.05);
}

.code-copy.elysia-style:hover {
  border-color: rgba(253, 186, 116, 0.3);
  background-color: var(--vp-c-bg-soft);
}
.dark .code-copy.elysia-style:hover {
  border-color: rgba(253, 186, 116, 0.3);
  background-color: #0a0a0a;
}
.code-copy.elysia-style code {
  color: #fb923c;
  font-family: var(--vp-font-family-mono);
  font-size: 1.05rem;
  background-color: transparent !important;
  padding: 0;
}
.code-copy.elysia-style code .dim {
  color: var(--vp-c-text-2);
}
.copy-btn {
  background: transparent;
  border: none;
  display: flex;
  align-items: center;
  color: var(--vp-c-text-3);
}
.copy-btn:hover {
  color: var(--vp-c-text-1);
}

.github-download.elysia-style {
  display: flex;
  align-items: center;
  justify-content: center;
  background-color: var(--vp-c-bg-mute); /* Light mode */
  border: 1px solid var(--vp-c-divider);
  padding: 12px;
  border-radius: 50%;
  color: var(--vp-c-text-2);
  cursor: pointer;
  transition: all 0.2s;
  text-decoration: none;
}
.dark .github-download.elysia-style {
  background-color: #050505;
  border-color: rgba(255, 255, 255, 0.05);
  color: var(--vp-c-text-2);
}
.github-download.elysia-style:hover {
  border-color: rgba(253, 186, 116, 0.3);
  background-color: var(--vp-c-bg-soft);
  color: var(--vp-c-text-1);
}
.dark .github-download.elysia-style:hover {
  border-color: rgba(253, 186, 116, 0.3);
  background-color: #0a0a0a;
  color: var(--vp-c-text-1);
}



.animated-logo {
  animation: pulse-logo 4s infinite ease-in-out;
}

@keyframes pulse-logo {
  0% { transform: scale(1); }
  50% { transform: scale(1.05); }
  100% { transform: scale(1); }
}
</style>

<style>
/* Forcefully prevent page scrolling on the home layout */
html:has(.hero), body:has(.hero) {
  overflow: hidden !important;
  height: 100vh !important;
}

/* Remove default VitePress bottom spacing */
#VPContent, .VPHome, .VPContent {
  padding-bottom: 0 !important;
  margin-bottom: 0 !important;
}
</style>
