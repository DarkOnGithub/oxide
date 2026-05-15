<template>
  <div class="benchmark-tables">
    <div class="controls">
      <div class="control-group">
        <label>Processeur :</label>
        <div class="select-wrapper">
          <select v-model="selectedCpu">
            <option v-for="cpu in Object.keys(data)" :key="cpu" :value="cpu">{{ cpu }}</option>
          </select>
        </div>
      </div>
      <div class="control-group">
        <label>Jeu de données :</label>
        <div class="select-wrapper">
          <select v-model="selectedDataset">
            <option v-for="ds in availableDatasets" :key="ds" :value="ds">{{ formatDataset(ds) }}</option>
          </select>
        </div>
      </div>
    </div>

    <div class="tables-container" v-if="currentData">
      <div class="table-section" v-if="currentData['Archive'] && currentData['Archive'].length > 0">
        <h3>Performances d'Archivage</h3>
        <div class="table-responsive">
          <table>
            <thead>
              <tr>
                <th>Outil</th>
                <th>Mode</th>
                <th class="numeric">Débit (MiB/s)</th>
                <th class="numeric">Temps (s)</th>
                <th class="numeric">Ratio</th>
                <th class="numeric">Pic RAM</th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="(row, idx) in currentData['Archive']" :key="idx" :class="{ 'highlight-row': row.tool === 'oxide' }">
                <td><span class="tool-badge" :class="row.tool">{{ row.tool }}</span></td>
                <td><span class="mode-badge" :class="row.mode">{{ row.mode }}</span></td>
                <td class="numeric highlight-val">{{ row.mib_s }}</td>
                <td class="numeric">{{ row.avg_s }}</td>
                <td class="numeric">{{ row.ratio }}</td>
                <td class="numeric">{{ row.peak_rss }}</td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>

      <div class="table-section" v-if="currentData['Extract'] && currentData['Extract'].length > 0">
        <h3>Performances d'Extraction</h3>
        <div class="table-responsive">
          <table>
            <thead>
              <tr>
                <th>Outil</th>
                <th>Mode</th>
                <th class="numeric">Débit (MiB/s)</th>
                <th class="numeric">Temps (s)</th>
                <th class="numeric">Pic RAM</th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="(row, idx) in currentData['Extract']" :key="idx" :class="{ 'highlight-row': row.tool === 'oxide' }">
                <td><span class="tool-badge" :class="row.tool">{{ row.tool }}</span></td>
                <td><span class="mode-badge" :class="row.mode">{{ row.mode }}</span></td>
                <td class="numeric highlight-val">{{ row.mib_s }}</td>
                <td class="numeric">{{ row.avg_s }}</td>
                <td class="numeric">{{ row.peak_rss }}</td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, computed, watch } from 'vue'

const data = {
  "9995WX": {
    "dataset_200mb": {
      "Archive": [
        {
          "tool": "oxide",
          "mode": "fast",
          "avg_s": "0.105",
          "mib_s": "1917.3",
          "ratio": "0.477",
          "peak_rss": "134.6 MB"
        },
        {
          "tool": "squashfs",
          "mode": "fast",
          "avg_s": "0.198",
          "mib_s": "1032.9",
          "ratio": "0.477",
          "peak_rss": "36.3 MB"
        },
        {
          "tool": "tar+zstd",
          "mode": "fast",
          "avg_s": "0.21",
          "mib_s": "963",
          "ratio": "0.346",
          "peak_rss": "61.3 MB"
        },
        {
          "tool": "oxide",
          "mode": "balanced",
          "avg_s": "0.157",
          "mib_s": "1289.4",
          "ratio": "0.329",
          "peak_rss": "135.8 MB"
        },
        {
          "tool": "squashfs",
          "mode": "balanced",
          "avg_s": "0.198",
          "mib_s": "1033.7",
          "ratio": "0.331",
          "peak_rss": "46.3 MB"
        },
        {
          "tool": "tar+zstd",
          "mode": "balanced",
          "avg_s": "0.23",
          "mib_s": "887.5",
          "ratio": "0.312",
          "peak_rss": "190.7 MB"
        },
        {
          "tool": "oxide",
          "mode": "ultra",
          "avg_s": "3.255",
          "mib_s": "62.1",
          "ratio": "0.249",
          "peak_rss": "908.9 MB"
        },
        {
          "tool": "7zip",
          "mode": "ultra",
          "avg_s": "6.414",
          "mib_s": "31.5",
          "ratio": "0.233",
          "peak_rss": "849.4 MB"
        },
        {
          "tool": "dwarfs",
          "mode": "ultra",
          "avg_s": "7.193",
          "mib_s": "28.1",
          "ratio": "0.251",
          "peak_rss": "3.5 GB"
        },
        {
          "tool": "pixz",
          "mode": "ultra",
          "avg_s": "28.569",
          "mib_s": "7.1",
          "ratio": "0.23",
          "peak_rss": "1.5 GB"
        }
      ],
      "Extract": [
        {
          "tool": "oxide",
          "mode": "fast",
          "avg_s": "0.153",
          "mib_s": "1330.7",
          "ratio": "1",
          "peak_rss": "200.7 MB"
        },
        {
          "tool": "squashfs",
          "mode": "fast",
          "avg_s": "0.157",
          "mib_s": "1295.5",
          "ratio": "1",
          "peak_rss": "179.7 MB"
        },
        {
          "tool": "tar+zstd",
          "mode": "fast",
          "avg_s": "0.304",
          "mib_s": "666.9",
          "ratio": "1",
          "peak_rss": "10.2 MB"
        },
        {
          "tool": "oxide",
          "mode": "balanced",
          "avg_s": "0.162",
          "mib_s": "1249.9",
          "ratio": "1",
          "peak_rss": "121.5 MB"
        },
        {
          "tool": "squashfs",
          "mode": "balanced",
          "avg_s": "0.157",
          "mib_s": "1290.8",
          "ratio": "1",
          "peak_rss": "222.2 MB"
        },
        {
          "tool": "tar+zstd",
          "mode": "balanced",
          "avg_s": "0.322",
          "mib_s": "628",
          "ratio": "1",
          "peak_rss": "11.7 MB"
        },
        {
          "tool": "oxide",
          "mode": "ultra",
          "avg_s": "0.256",
          "mib_s": "802.6",
          "ratio": "1",
          "peak_rss": "250.4 MB"
        },
        {
          "tool": "7zip",
          "mode": "ultra",
          "avg_s": "0.547",
          "mib_s": "369.6",
          "ratio": "1",
          "peak_rss": "255.9 MB"
        },
        {
          "tool": "dwarfs",
          "mode": "ultra",
          "avg_s": "0.214",
          "mib_s": "945.5",
          "ratio": "1",
          "peak_rss": "198.3 MB"
        },
        {
          "tool": "pixz",
          "mode": "ultra",
          "avg_s": "1.294",
          "mib_s": "156.3",
          "ratio": "1",
          "peak_rss": "324.3 MB"
        }
      ]
    },
    "dataset_6gb": {
      "Archive": [
        {
          "tool": "oxide",
          "mode": "fast",
          "avg_s": "3.45",
          "mib_s": "1699",
          "ratio": "0.727",
          "peak_rss": "4.5 GB"
        },
        {
          "tool": "squashfs",
          "mode": "fast",
          "avg_s": "7.329",
          "mib_s": "800",
          "ratio": "0.727",
          "peak_rss": "1.3 GB"
        },
        {
          "tool": "tar+zstd",
          "mode": "fast",
          "avg_s": "8.334",
          "mib_s": "705.2",
          "ratio": "0.672",
          "peak_rss": "69.8 MB"
        },
        {
          "tool": "oxide",
          "mode": "balanced",
          "avg_s": "3.931",
          "mib_s": "1490.4",
          "ratio": "0.665",
          "peak_rss": "4.5 GB"
        },
        {
          "tool": "squashfs",
          "mode": "balanced",
          "avg_s": "7.679",
          "mib_s": "763.8",
          "ratio": "0.666",
          "peak_rss": "1.3 GB"
        },
        {
          "tool": "tar+zstd",
          "mode": "balanced",
          "avg_s": "8.007",
          "mib_s": "732.2",
          "ratio": "0.659",
          "peak_rss": "220.7 MB"
        },
        {
          "tool": "oxide",
          "mode": "ultra",
          "avg_s": "34.826",
          "mib_s": "168.1",
          "ratio": "0.637",
          "peak_rss": "3.2 GB"
        },
        {
          "tool": "7zip",
          "mode": "ultra",
          "avg_s": "85.962",
          "mib_s": "68.1",
          "ratio": "0.629",
          "peak_rss": "1.3 GB"
        },
        {
          "tool": "dwarfs",
          "mode": "ultra",
          "avg_s": "47.47",
          "mib_s": "123.3",
          "ratio": "0.634",
          "peak_rss": "29.2 GB"
        },
        {
          "tool": "pixz",
          "mode": "ultra",
          "avg_s": "251.439",
          "mib_s": "23.3",
          "ratio": "0.626",
          "peak_rss": "11.3 GB"
        }
      ],
      "Extract": [
        {
          "tool": "oxide",
          "mode": "fast",
          "avg_s": "5.622",
          "mib_s": "1042",
          "ratio": "1",
          "peak_rss": "3.0 GB"
        },
        {
          "tool": "squashfs",
          "mode": "fast",
          "avg_s": "7.887",
          "mib_s": "742.5",
          "ratio": "1",
          "peak_rss": "546.2 MB"
        },
        {
          "tool": "tar+zstd",
          "mode": "fast",
          "avg_s": "11.064",
          "mib_s": "529.1",
          "ratio": "1",
          "peak_rss": "10.1 MB"
        },
        {
          "tool": "oxide",
          "mode": "balanced",
          "avg_s": "5.715",
          "mib_s": "1024.3",
          "ratio": "1",
          "peak_rss": "3.3 GB"
        },
        {
          "tool": "squashfs",
          "mode": "balanced",
          "avg_s": "8.036",
          "mib_s": "728.4",
          "ratio": "1",
          "peak_rss": "548.8 MB"
        },
        {
          "tool": "tar+zstd",
          "mode": "balanced",
          "avg_s": "11.134",
          "mib_s": "525.9",
          "ratio": "1",
          "peak_rss": "11.7 MB"
        },
        {
          "tool": "oxide",
          "mode": "ultra",
          "avg_s": "5.878",
          "mib_s": "995.8",
          "ratio": "1",
          "peak_rss": "1.2 GB"
        },
        {
          "tool": "7zip",
          "mode": "ultra",
          "avg_s": "17.929",
          "mib_s": "326.5",
          "ratio": "1",
          "peak_rss": "1.0 GB"
        },
        {
          "tool": "dwarfs",
          "mode": "ultra",
          "avg_s": "12.45",
          "mib_s": "470.2",
          "ratio": "1",
          "peak_rss": "1.8 GB"
        },
        {
          "tool": "pixz",
          "mode": "ultra",
          "avg_s": "12.396",
          "mib_s": "472.2",
          "ratio": "1",
          "peak_rss": "9.5 GB"
        }
      ]
    },
    "dataset_6gb_linux": {
      "Archive": [
        {
          "tool": "oxide",
          "mode": "fast",
          "avg_s": "2.746",
          "mib_s": "516.6",
          "ratio": "0.273",
          "peak_rss": "254.2 MB"
        },
        {
          "tool": "squashfs",
          "mode": "fast",
          "avg_s": "4.953",
          "mib_s": "286",
          "ratio": "0.275",
          "peak_rss": "1.1 GB"
        },
        {
          "tool": "tar+zstd",
          "mode": "fast",
          "avg_s": "4.674",
          "mib_s": "303.1",
          "ratio": "0.169",
          "peak_rss": "52.0 MB"
        },
        {
          "tool": "oxide",
          "mode": "balanced",
          "avg_s": "3.252",
          "mib_s": "435.8",
          "ratio": "0.161",
          "peak_rss": "285.6 MB"
        },
        {
          "tool": "squashfs",
          "mode": "balanced",
          "avg_s": "4.964",
          "mib_s": "285.4",
          "ratio": "0.164",
          "peak_rss": "1.1 GB"
        },
        {
          "tool": "tar+zstd",
          "mode": "balanced",
          "avg_s": "4.732",
          "mib_s": "299.7",
          "ratio": "0.149",
          "peak_rss": "170.8 MB"
        },
        {
          "tool": "oxide",
          "mode": "ultra",
          "avg_s": "16.468",
          "mib_s": "85.8",
          "ratio": "0.109",
          "peak_rss": "1.9 GB"
        },
        {
          "tool": "7zip",
          "mode": "ultra",
          "avg_s": "27.945",
          "mib_s": "50.6",
          "ratio": "0.101",
          "peak_rss": "1.1 GB"
        },
        {
          "tool": "dwarfs",
          "mode": "ultra",
          "avg_s": "19.905",
          "mib_s": "71",
          "ratio": "0.113",
          "peak_rss": "14.4 GB"
        },
        {
          "tool": "pixz",
          "mode": "ultra",
          "avg_s": "44.821",
          "mib_s": "31.5",
          "ratio": "0.097",
          "peak_rss": "8.2 GB"
        }
      ],
      "Extract": [
        {
          "tool": "oxide",
          "mode": "fast",
          "avg_s": "4.51",
          "mib_s": "313.5",
          "ratio": "1",
          "peak_rss": "1.3 GB"
        },
        {
          "tool": "squashfs",
          "mode": "fast",
          "avg_s": "5.945",
          "mib_s": "238",
          "ratio": "1",
          "peak_rss": "535.4 MB"
        },
        {
          "tool": "tar+zstd",
          "mode": "fast",
          "avg_s": "7.131",
          "mib_s": "198.3",
          "ratio": "1",
          "peak_rss": "10.1 MB"
        },
        {
          "tool": "oxide",
          "mode": "balanced",
          "avg_s": "4.482",
          "mib_s": "315.7",
          "ratio": "1",
          "peak_rss": "1.3 GB"
        },
        {
          "tool": "squashfs",
          "mode": "balanced",
          "avg_s": "5.89",
          "mib_s": "240.1",
          "ratio": "1",
          "peak_rss": "536.1 MB"
        },
        {
          "tool": "tar+zstd",
          "mode": "balanced",
          "avg_s": "6.792",
          "mib_s": "211.2",
          "ratio": "1",
          "peak_rss": "11.5 MB"
        },
        {
          "tool": "oxide",
          "mode": "ultra",
          "avg_s": "4.571",
          "mib_s": "309.3",
          "ratio": "1",
          "peak_rss": "1.1 GB"
        },
        {
          "tool": "7zip",
          "mode": "ultra",
          "avg_s": "8.94",
          "mib_s": "158.1",
          "ratio": "1",
          "peak_rss": "613.8 MB"
        },
        {
          "tool": "dwarfs",
          "mode": "ultra",
          "avg_s": "9.297",
          "mib_s": "152.1",
          "ratio": "1",
          "peak_rss": "715.0 MB"
        },
        {
          "tool": "pixz",
          "mode": "ultra",
          "avg_s": "8.106",
          "mib_s": "174.4",
          "ratio": "1",
          "peak_rss": "1.8 GB"
        }
      ]
    }
  },
  "7950x": {
    "dataset_200mb": {
      "Archive": [
        {
          "tool": "oxide",
          "mode": "fast",
          "avg_s": "0.105",
          "mib_s": "1926.5",
          "ratio": "0.477",
          "peak_rss": "125.8 MB"
        },
        {
          "tool": "squashfs",
          "mode": "fast",
          "avg_s": "0.103",
          "mib_s": "1954.3",
          "ratio": "0.477",
          "peak_rss": "43.8 MB"
        },
        {
          "tool": "tar+zstd",
          "mode": "fast",
          "avg_s": "0.156",
          "mib_s": "1294.6",
          "ratio": "0.346",
          "peak_rss": "57.8 MB"
        },
        {
          "tool": "oxide",
          "mode": "balanced",
          "avg_s": "0.104",
          "mib_s": "1946.6",
          "ratio": "0.329",
          "peak_rss": "127.3 MB"
        },
        {
          "tool": "squashfs",
          "mode": "balanced",
          "avg_s": "0.104",
          "mib_s": "1944.9",
          "ratio": "0.331",
          "peak_rss": "55.9 MB"
        },
        {
          "tool": "tar+zstd",
          "mode": "balanced",
          "avg_s": "0.198",
          "mib_s": "1036.9",
          "ratio": "0.312",
          "peak_rss": "193.6 MB"
        },
        {
          "tool": "oxide",
          "mode": "ultra",
          "avg_s": "3.101",
          "mib_s": "65.2",
          "ratio": "0.249",
          "peak_rss": "1.5 GB"
        },
        {
          "tool": "7zip",
          "mode": "ultra",
          "avg_s": "8.11",
          "mib_s": "24.9",
          "ratio": "0.233",
          "peak_rss": "849.9 MB"
        },
        {
          "tool": "dwarfs",
          "mode": "ultra",
          "avg_s": "8.01",
          "mib_s": "25.2",
          "ratio": "0.251",
          "peak_rss": "3.5 GB"
        },
        {
          "tool": "pixz",
          "mode": "ultra",
          "avg_s": "29.338",
          "mib_s": "6.9",
          "ratio": "0.23",
          "peak_rss": "1.5 GB"
        }
      ],
      "Extract": [
        {
          "tool": "oxide",
          "mode": "fast",
          "avg_s": "0.143",
          "mib_s": "1417.3",
          "ratio": "1",
          "peak_rss": "256.1 MB"
        },
        {
          "tool": "squashfs",
          "mode": "fast",
          "avg_s": "0.141",
          "mib_s": "1433.6",
          "ratio": "1",
          "peak_rss": "195.0 MB"
        },
        {
          "tool": "tar+zstd",
          "mode": "fast",
          "avg_s": "0.191",
          "mib_s": "1055.6",
          "ratio": "1",
          "peak_rss": "11.3 MB"
        },
        {
          "tool": "oxide",
          "mode": "balanced",
          "avg_s": "0.141",
          "mib_s": "1437.4",
          "ratio": "1",
          "peak_rss": "233.4 MB"
        },
        {
          "tool": "squashfs",
          "mode": "balanced",
          "avg_s": "0.14",
          "mib_s": "1443.3",
          "ratio": "1",
          "peak_rss": "221.5 MB"
        },
        {
          "tool": "tar+zstd",
          "mode": "balanced",
          "avg_s": "0.191",
          "mib_s": "1060",
          "ratio": "1",
          "peak_rss": "12.8 MB"
        },
        {
          "tool": "oxide",
          "mode": "ultra",
          "avg_s": "0.196",
          "mib_s": "1032.4",
          "ratio": "1",
          "peak_rss": "319.2 MB"
        },
        {
          "tool": "7zip",
          "mode": "ultra",
          "avg_s": "0.45",
          "mib_s": "449.5",
          "ratio": "1",
          "peak_rss": "255.1 MB"
        },
        {
          "tool": "dwarfs",
          "mode": "ultra",
          "avg_s": "0.142",
          "mib_s": "1424.7",
          "ratio": "1",
          "peak_rss": "197.3 MB"
        },
        {
          "tool": "pixz",
          "mode": "ultra",
          "avg_s": "1.187",
          "mib_s": "170.3",
          "ratio": "1",
          "peak_rss": "323.9 MB"
        }
      ]
    },
    "dataset_6gb": {
      "Archive": [
        {
          "tool": "oxide",
          "mode": "fast",
          "avg_s": "5.137",
          "mib_s": "1139.9",
          "ratio": "0.727",
          "peak_rss": "4.2 GB"
        },
        {
          "tool": "squashfs",
          "mode": "fast",
          "avg_s": "8.619",
          "mib_s": "679.1",
          "ratio": "0.727",
          "peak_rss": "1.3 GB"
        },
        {
          "tool": "tar+zstd",
          "mode": "fast",
          "avg_s": "10.31",
          "mib_s": "567.8",
          "ratio": "0.672",
          "peak_rss": "61.8 MB"
        },
        {
          "tool": "oxide",
          "mode": "balanced",
          "avg_s": "5.458",
          "mib_s": "1072.5",
          "ratio": "0.665",
          "peak_rss": "4.1 GB"
        },
        {
          "tool": "squashfs",
          "mode": "balanced",
          "avg_s": "8.731",
          "mib_s": "670.5",
          "ratio": "0.666",
          "peak_rss": "1.3 GB"
        },
        {
          "tool": "tar+zstd",
          "mode": "balanced",
          "avg_s": "10.693",
          "mib_s": "547.4",
          "ratio": "0.659",
          "peak_rss": "231.6 MB"
        },
        {
          "tool": "oxide",
          "mode": "ultra",
          "avg_s": "42.191",
          "mib_s": "138.8",
          "ratio": "0.637",
          "peak_rss": "3.2 GB"
        },
        {
          "tool": "7zip",
          "mode": "ultra",
          "avg_s": "136.473",
          "mib_s": "42.9",
          "ratio": "0.629",
          "peak_rss": "1.4 GB"
        },
        {
          "tool": "dwarfs",
          "mode": "ultra",
          "avg_s": "103.21",
          "mib_s": "56.7",
          "ratio": "0.634",
          "peak_rss": "9.1 GB"
        },
        {
          "tool": "pixz",
          "mode": "ultra",
          "avg_s": "297.336",
          "mib_s": "19.7",
          "ratio": "0.626",
          "peak_rss": "11.3 GB"
        }
      ],
      "Extract": [
        {
          "tool": "oxide",
          "mode": "fast",
          "avg_s": "5.233",
          "mib_s": "1118.9",
          "ratio": "1",
          "peak_rss": "1.8 GB"
        },
        {
          "tool": "squashfs",
          "mode": "fast",
          "avg_s": "5.57",
          "mib_s": "1051",
          "ratio": "1",
          "peak_rss": "546.0 MB"
        },
        {
          "tool": "tar+zstd",
          "mode": "fast",
          "avg_s": "7.823",
          "mib_s": "748.3",
          "ratio": "1",
          "peak_rss": "11.3 MB"
        },
        {
          "tool": "oxide",
          "mode": "balanced",
          "avg_s": "5.008",
          "mib_s": "1168.7",
          "ratio": "1",
          "peak_rss": "1.9 GB"
        },
        {
          "tool": "squashfs",
          "mode": "balanced",
          "avg_s": "5.523",
          "mib_s": "1060",
          "ratio": "1",
          "peak_rss": "547.8 MB"
        },
        {
          "tool": "tar+zstd",
          "mode": "balanced",
          "avg_s": "7.955",
          "mib_s": "735.8",
          "ratio": "1",
          "peak_rss": "12.6 MB"
        },
        {
          "tool": "oxide",
          "mode": "ultra",
          "avg_s": "5.134",
          "mib_s": "1140.4",
          "ratio": "1",
          "peak_rss": "1.1 GB"
        },
        {
          "tool": "7zip",
          "mode": "ultra",
          "avg_s": "13.258",
          "mib_s": "441.5",
          "ratio": "1",
          "peak_rss": "1.0 GB"
        },
        {
          "tool": "dwarfs",
          "mode": "ultra",
          "avg_s": "8.292",
          "mib_s": "715.7",
          "ratio": "1",
          "peak_rss": "1.8 GB"
        },
        {
          "tool": "pixz",
          "mode": "ultra",
          "avg_s": "9.725",
          "mib_s": "601.9",
          "ratio": "1",
          "peak_rss": "9.2 GB"
        }
      ]
    },
    "dataset_6gb_linux": {
      "Archive": [
        {
          "tool": "oxide",
          "mode": "fast",
          "avg_s": "3.981",
          "mib_s": "370.9",
          "ratio": "0.273",
          "peak_rss": "261.0 MB"
        },
        {
          "tool": "squashfs",
          "mode": "fast",
          "avg_s": "6.99",
          "mib_s": "202.3",
          "ratio": "0.275",
          "peak_rss": "1.1 GB"
        },
        {
          "tool": "tar+zstd",
          "mode": "fast",
          "avg_s": "7.827",
          "mib_s": "180.7",
          "ratio": "0.169",
          "peak_rss": "49.3 MB"
        },
        {
          "tool": "oxide",
          "mode": "balanced",
          "avg_s": "4.242",
          "mib_s": "336.9",
          "ratio": "0.161",
          "peak_rss": "252.3 MB"
        },
        {
          "tool": "squashfs",
          "mode": "balanced",
          "avg_s": "6.779",
          "mib_s": "210.4",
          "ratio": "0.164",
          "peak_rss": "1.1 GB"
        },
        {
          "tool": "tar+zstd",
          "mode": "balanced",
          "avg_s": "7.913",
          "mib_s": "178.7",
          "ratio": "0.149",
          "peak_rss": "166.6 MB"
        },
        {
          "tool": "oxide",
          "mode": "ultra",
          "avg_s": "14.594",
          "mib_s": "96.9",
          "ratio": "0.109",
          "peak_rss": "2.7 GB"
        },
        {
          "tool": "7zip",
          "mode": "ultra",
          "avg_s": "27.343",
          "mib_s": "51.7",
          "ratio": "0.101",
          "peak_rss": "2.0 GB"
        },
        {
          "tool": "dwarfs",
          "mode": "ultra",
          "avg_s": "30.089",
          "mib_s": "47",
          "ratio": "0.113",
          "peak_rss": "8.9 GB"
        },
        {
          "tool": "pixz",
          "mode": "ultra",
          "avg_s": "57.369",
          "mib_s": "24.6",
          "ratio": "0.097",
          "peak_rss": "7.6 GB"
        }
      ],
      "Extract": [
        {
          "tool": "oxide",
          "mode": "fast",
          "avg_s": "3.325",
          "mib_s": "425.2",
          "ratio": "1",
          "peak_rss": "1.2 GB"
        },
        {
          "tool": "squashfs",
          "mode": "fast",
          "avg_s": "4.129",
          "mib_s": "342.4",
          "ratio": "1",
          "peak_rss": "550.5 MB"
        },
        {
          "tool": "tar+zstd",
          "mode": "fast",
          "avg_s": "5.792",
          "mib_s": "244.1",
          "ratio": "1",
          "peak_rss": "11.3 MB"
        },
        {
          "tool": "oxide",
          "mode": "balanced",
          "avg_s": "3.182",
          "mib_s": "444.2",
          "ratio": "1",
          "peak_rss": "1.2 GB"
        },
        {
          "tool": "squashfs",
          "mode": "balanced",
          "avg_s": "4.167",
          "mib_s": "339.2",
          "ratio": "1",
          "peak_rss": "551.3 MB"
        },
        {
          "tool": "tar+zstd",
          "mode": "balanced",
          "avg_s": "5.806",
          "mib_s": "243.5",
          "ratio": "1",
          "peak_rss": "12.6 MB"
        },
        {
          "tool": "oxide",
          "mode": "ultra",
          "avg_s": "3.309",
          "mib_s": "427.3",
          "ratio": "1",
          "peak_rss": "1.2 GB"
        },
        {
          "tool": "7zip",
          "mode": "ultra",
          "avg_s": "7.395",
          "mib_s": "191.2",
          "ratio": "1",
          "peak_rss": "1.1 GB"
        },
        {
          "tool": "dwarfs",
          "mode": "ultra",
          "avg_s": "7.169",
          "mib_s": "197.2",
          "ratio": "1",
          "peak_rss": "714.8 MB"
        },
        {
          "tool": "pixz",
          "mode": "ultra",
          "avg_s": "6.671",
          "mib_s": "211.9",
          "ratio": "1",
          "peak_rss": "1.8 GB"
        }
      ]
    }
  },
  "7995x": {
    "dataset_200mb": {
      "Archive": [
        {
          "tool": "oxide",
          "mode": "fast",
          "avg_s": "0.156",
          "mib_s": "1291.6",
          "ratio": "0.477",
          "peak_rss": "136.1 MB"
        },
        {
          "tool": "squashfs",
          "mode": "fast",
          "avg_s": "0.177",
          "mib_s": "1164.3",
          "ratio": "0.477",
          "peak_rss": "116.4 MB"
        },
        {
          "tool": "tar+zstd",
          "mode": "fast",
          "avg_s": "0.189",
          "mib_s": "1092.1",
          "ratio": "0.346",
          "peak_rss": "220.4 MB"
        },
        {
          "tool": "oxide",
          "mode": "balanced",
          "avg_s": "0.156",
          "mib_s": "1295.3",
          "ratio": "0.329",
          "peak_rss": "135.6 MB"
        },
        {
          "tool": "squashfs",
          "mode": "balanced",
          "avg_s": "0.188",
          "mib_s": "1098",
          "ratio": "0.331",
          "peak_rss": "145.8 MB"
        },
        {
          "tool": "tar+zstd",
          "mode": "balanced",
          "avg_s": "0.21",
          "mib_s": "963.5",
          "ratio": "0.312",
          "peak_rss": "249.5 MB"
        },
        {
          "tool": "oxide",
          "mode": "ultra",
          "avg_s": "1.757",
          "mib_s": "118.6",
          "ratio": "0.249",
          "peak_rss": "2.9 GB"
        },
        {
          "tool": "7zip",
          "mode": "ultra",
          "avg_s": "7.894",
          "mib_s": "25.6",
          "ratio": "0.233",
          "peak_rss": "853.5 MB"
        },
        {
          "tool": "dwarfs",
          "mode": "ultra",
          "avg_s": "8.585",
          "mib_s": "23.6",
          "ratio": "0.251",
          "peak_rss": "3.5 GB"
        },
        {
          "tool": "pixz",
          "mode": "ultra",
          "avg_s": "31.593",
          "mib_s": "6.4",
          "ratio": "0.23",
          "peak_rss": "1.5 GB"
        }
      ],
      "Extract": [
        {
          "tool": "oxide",
          "mode": "fast",
          "avg_s": "0.129",
          "mib_s": "1567.5",
          "ratio": "1",
          "peak_rss": "235.5 MB"
        },
        {
          "tool": "squashfs",
          "mode": "fast",
          "avg_s": "0.137",
          "mib_s": "1499.5",
          "ratio": "1",
          "peak_rss": "317.2 MB"
        },
        {
          "tool": "tar+zstd",
          "mode": "fast",
          "avg_s": "0.245",
          "mib_s": "823.7",
          "ratio": "1",
          "peak_rss": "12.1 MB"
        },
        {
          "tool": "oxide",
          "mode": "balanced",
          "avg_s": "0.128",
          "mib_s": "1580.6",
          "ratio": "1",
          "peak_rss": "192.6 MB"
        },
        {
          "tool": "squashfs",
          "mode": "balanced",
          "avg_s": "0.14",
          "mib_s": "1474.3",
          "ratio": "1",
          "peak_rss": "390.9 MB"
        },
        {
          "tool": "tar+zstd",
          "mode": "balanced",
          "avg_s": "0.256",
          "mib_s": "795",
          "ratio": "1",
          "peak_rss": "13.6 MB"
        },
        {
          "tool": "oxide",
          "mode": "ultra",
          "avg_s": "0.183",
          "mib_s": "1107.3",
          "ratio": "1",
          "peak_rss": "283.6 MB"
        },
        {
          "tool": "7zip",
          "mode": "ultra",
          "avg_s": "0.496",
          "mib_s": "407.8",
          "ratio": "1",
          "peak_rss": "256.6 MB"
        },
        {
          "tool": "dwarfs",
          "mode": "ultra",
          "avg_s": "0.132",
          "mib_s": "1526.1",
          "ratio": "1",
          "peak_rss": "234.3 MB"
        },
        {
          "tool": "pixz",
          "mode": "ultra",
          "avg_s": "1.248",
          "mib_s": "162",
          "ratio": "1",
          "peak_rss": "326.1 MB"
        }
      ]
    },
    "dataset_6gb": {
      "Archive": [
        {
          "tool": "oxide",
          "mode": "fast",
          "avg_s": "8.802",
          "mib_s": "665.4",
          "ratio": "0.727",
          "peak_rss": "2.5 GB"
        },
        {
          "tool": "squashfs",
          "mode": "fast",
          "avg_s": "6.596",
          "mib_s": "887.4",
          "ratio": "0.727",
          "peak_rss": "1.4 GB"
        },
        {
          "tool": "tar+zstd",
          "mode": "fast",
          "avg_s": "6.997",
          "mib_s": "837",
          "ratio": "0.672",
          "peak_rss": "417.8 MB"
        },
        {
          "tool": "oxide",
          "mode": "balanced",
          "avg_s": "8.177",
          "mib_s": "773.5",
          "ratio": "0.665",
          "peak_rss": "3.0 GB"
        },
        {
          "tool": "squashfs",
          "mode": "balanced",
          "avg_s": "7.081",
          "mib_s": "826.9",
          "ratio": "0.666",
          "peak_rss": "1.6 GB"
        },
        {
          "tool": "tar+zstd",
          "mode": "balanced",
          "avg_s": "6.982",
          "mib_s": "838.5",
          "ratio": "0.659",
          "peak_rss": "1.6 GB"
        },
        {
          "tool": "oxide",
          "mode": "ultra",
          "avg_s": "13.068",
          "mib_s": "453.7",
          "ratio": "0.637",
          "peak_rss": "9.3 GB"
        },
        {
          "tool": "7zip",
          "mode": "ultra",
          "avg_s": "42.611",
          "mib_s": "137.4",
          "ratio": "0.629",
          "peak_rss": "9.1 GB"
        },
        {
          "tool": "dwarfs",
          "mode": "ultra",
          "avg_s": "46.386",
          "mib_s": "126.2",
          "ratio": "0.634",
          "peak_rss": "31.0 GB"
        },
        {
          "tool": "pixz",
          "mode": "ultra",
          "avg_s": "116.686",
          "mib_s": "50.2",
          "ratio": "0.626",
          "peak_rss": "35.0 GB"
        }
      ],
      "Extract": [
        {
          "tool": "oxide",
          "mode": "fast",
          "avg_s": "3.441",
          "mib_s": "1701.1",
          "ratio": "1",
          "peak_rss": "2.5 GB"
        },
        {
          "tool": "squashfs",
          "mode": "fast",
          "avg_s": "5.182",
          "mib_s": "1129.9",
          "ratio": "1",
          "peak_rss": "727.8 MB"
        },
        {
          "tool": "tar+zstd",
          "mode": "fast",
          "avg_s": "9.069",
          "mib_s": "645.5",
          "ratio": "1",
          "peak_rss": "12.1 MB"
        },
        {
          "tool": "oxide",
          "mode": "balanced",
          "avg_s": "3.396",
          "mib_s": "1723.8",
          "ratio": "1",
          "peak_rss": "2.3 GB"
        },
        {
          "tool": "squashfs",
          "mode": "balanced",
          "avg_s": "5.19",
          "mib_s": "1128",
          "ratio": "1",
          "peak_rss": "739.3 MB"
        },
        {
          "tool": "tar+zstd",
          "mode": "balanced",
          "avg_s": "9.106",
          "mib_s": "642.9",
          "ratio": "1",
          "peak_rss": "13.7 MB"
        },
        {
          "tool": "oxide",
          "mode": "ultra",
          "avg_s": "3.52",
          "mib_s": "1662.8",
          "ratio": "1",
          "peak_rss": "3.1 GB"
        },
        {
          "tool": "7zip",
          "mode": "ultra",
          "avg_s": "13.419",
          "mib_s": "436.2",
          "ratio": "1",
          "peak_rss": "2.0 GB"
        },
        {
          "tool": "dwarfs",
          "mode": "ultra",
          "avg_s": "8.87",
          "mib_s": "659.9",
          "ratio": "1",
          "peak_rss": "1.8 GB"
        },
        {
          "tool": "pixz",
          "mode": "ultra",
          "avg_s": "10.874",
          "mib_s": "538.4",
          "ratio": "1",
          "peak_rss": "9.5 GB"
        }
      ]
    },
    "dataset_6gb_linux": {
      "Archive": [
        {
          "tool": "oxide",
          "mode": "fast",
          "avg_s": "4.383",
          "mib_s": "322.6",
          "ratio": "0.273",
          "peak_rss": "178.0 MB"
        },
        {
          "tool": "squashfs",
          "mode": "fast",
          "avg_s": "4.576",
          "mib_s": "308.9",
          "ratio": "0.275",
          "peak_rss": "1.1 GB"
        },
        {
          "tool": "tar+zstd",
          "mode": "fast",
          "avg_s": "4.271",
          "mib_s": "331.1",
          "ratio": "0.169",
          "peak_rss": "405.8 MB"
        },
        {
          "tool": "oxide",
          "mode": "balanced",
          "avg_s": "5.37",
          "mib_s": "280.8",
          "ratio": "0.161",
          "peak_rss": "306.6 MB"
        },
        {
          "tool": "squashfs",
          "mode": "balanced",
          "avg_s": "4.6",
          "mib_s": "307.3",
          "ratio": "0.164",
          "peak_rss": "1.2 GB"
        },
        {
          "tool": "tar+zstd",
          "mode": "balanced",
          "avg_s": "4.285",
          "mib_s": "329.9",
          "ratio": "0.149",
          "peak_rss": "1.5 GB"
        },
        {
          "tool": "oxide",
          "mode": "ultra",
          "avg_s": "9.078",
          "mib_s": "155.7",
          "ratio": "0.109",
          "peak_rss": "6.0 GB"
        },
        {
          "tool": "7zip",
          "mode": "ultra",
          "avg_s": "14.98",
          "mib_s": "94.4",
          "ratio": "0.101",
          "peak_rss": "5.3 GB"
        },
        {
          "tool": "dwarfs",
          "mode": "ultra",
          "avg_s": "18.683",
          "mib_s": "75.7",
          "ratio": "0.113",
          "peak_rss": "15.5 GB"
        },
        {
          "tool": "pixz",
          "mode": "ultra",
          "avg_s": "56.356",
          "mib_s": "25.1",
          "ratio": "0.097",
          "peak_rss": "7.9 GB"
        }
      ],
      "Extract": [
        {
          "tool": "oxide",
          "mode": "fast",
          "avg_s": "2.635",
          "mib_s": "536.7",
          "ratio": "1",
          "peak_rss": "1.6 GB"
        },
        {
          "tool": "squashfs",
          "mode": "fast",
          "avg_s": "3.632",
          "mib_s": "389.3",
          "ratio": "1",
          "peak_rss": "716.2 MB"
        },
        {
          "tool": "tar+zstd",
          "mode": "fast",
          "avg_s": "5.454",
          "mib_s": "259.2",
          "ratio": "1",
          "peak_rss": "12.1 MB"
        },
        {
          "tool": "oxide",
          "mode": "balanced",
          "avg_s": "2.569",
          "mib_s": "550.4",
          "ratio": "1",
          "peak_rss": "1.5 GB"
        },
        {
          "tool": "squashfs",
          "mode": "balanced",
          "avg_s": "3.652",
          "mib_s": "387",
          "ratio": "1",
          "peak_rss": "724.4 MB"
        },
        {
          "tool": "tar+zstd",
          "mode": "balanced",
          "avg_s": "5.423",
          "mib_s": "260.7",
          "ratio": "1",
          "peak_rss": "13.6 MB"
        },
        {
          "tool": "oxide",
          "mode": "ultra",
          "avg_s": "2.657",
          "mib_s": "532.1",
          "ratio": "1",
          "peak_rss": "2.1 GB"
        },
        {
          "tool": "7zip",
          "mode": "ultra",
          "avg_s": "7.086",
          "mib_s": "199.5",
          "ratio": "1",
          "peak_rss": "1.1 GB"
        },
        {
          "tool": "dwarfs",
          "mode": "ultra",
          "avg_s": "6.922",
          "mib_s": "204.2",
          "ratio": "1",
          "peak_rss": "733.0 MB"
        },
        {
          "tool": "pixz",
          "mode": "ultra",
          "avg_s": "6.343",
          "mib_s": "222.9",
          "ratio": "1",
          "peak_rss": "1.8 GB"
        }
      ]
    }
  }
}

const selectedCpu = ref('7950x')
const selectedDataset = ref('dataset_6gb')

const availableDatasets = computed(() => {
  return data[selectedCpu.value] ? Object.keys(data[selectedCpu.value]) : []
})

const currentData = computed(() => {
  if (data[selectedCpu.value] && data[selectedCpu.value][selectedDataset.value]) {
    return data[selectedCpu.value][selectedDataset.value]
  }
  return null
})

watch(selectedCpu, (newCpu) => {
  if (data[newCpu] && !data[newCpu][selectedDataset.value]) {
    selectedDataset.value = Object.keys(data[newCpu])[0]
  }
})

const formatDataset = (ds) => {
  if (ds === 'dataset_6gb') return '6GB Mixed'
  if (ds === 'dataset_6gb_linux') return '1.4GB Linux Kernel'
  if (ds === 'dataset_200mb') return '200MB Silesia'
  return ds
}
</script>

<style scoped>
.benchmark-tables {
  margin: 3rem 0;
  display: flex;
  flex-direction: column;
  gap: 2rem;
}
.controls {
  display: flex;
  flex-wrap: wrap;
  gap: 2rem;
  padding: 1.5rem 2rem;
  background: var(--vp-c-bg-soft);
  border: 1px solid var(--vp-c-divider);
  border-radius: 12px;
  box-shadow: 0 4px 15px rgba(0, 0, 0, 0.05);
}
.dark .controls {
  background: #16151b;
  border-color: rgba(255, 255, 255, 0.05);
  box-shadow: 0 4px 20px rgba(0, 0, 0, 0.2);
}
.control-group {
  display: flex;
  align-items: center;
  gap: 1rem;
}
.control-group label {
  font-weight: 600;
  font-size: 0.95rem;
  color: var(--vp-c-text-2);
}
.dark .control-group label {
  color: #a3a3a3;
}
.select-wrapper {
  position: relative;
}
select {
  appearance: none;
  padding: 0.6rem 2.5rem 0.6rem 1.2rem;
  border-radius: 8px;
  border: 1px solid var(--vp-c-divider);
  background-color: var(--vp-c-bg);
  color: var(--vp-c-text-1);
  font-family: var(--vp-font-family-base);
  font-weight: 600;
  font-size: 0.9rem;
  outline: none;
  cursor: pointer;
  transition: all 0.2s ease;
  min-width: 160px;
}
.select-wrapper::after {
  content: '▼';
  font-size: 0.7rem;
  position: absolute;
  right: 1rem;
  top: 50%;
  transform: translateY(-50%);
  pointer-events: none;
  color: var(--vp-c-text-3);
}
select:hover {
  border-color: var(--vp-c-brand-1);
}
.dark select {
  background-color: #1a1a24;
  border-color: rgba(255, 255, 255, 0.1);
  color: #f3ece6;
}
.dark select:hover {
  border-color: rgba(251, 146, 60, 0.5);
}
.tables-container {
  display: flex;
  flex-direction: column;
  gap: 3rem;
}
h3 {
  margin: 0 0 1rem 0;
  font-size: 1.4rem;
  font-weight: 700;
  color: var(--vp-c-brand-1);
}
.dark h3 {
  background: linear-gradient(120deg, #fdba74, #fb923c);
  -webkit-background-clip: text;
  -webkit-text-fill-color: transparent;
  background-clip: text;
}
.table-responsive {
  overflow-x: auto;
  border-radius: 12px;
  border: 1px solid var(--vp-c-divider);
  background: var(--vp-c-bg-soft);
  box-shadow: 0 4px 15px rgba(0, 0, 0, 0.03);
}
.dark .table-responsive {
  border-color: rgba(255, 255, 255, 0.05);
  background: #16151b;
  box-shadow: 0 8px 30px rgba(0, 0, 0, 0.2);
}
table {
  width: 100%;
  border-collapse: collapse;
  font-size: 0.95rem;
  white-space: nowrap;
}
th, td {
  padding: 1rem 1.2rem;
  text-align: left;
  border-bottom: 1px solid var(--vp-c-divider);
}
.dark th, .dark td {
  border-bottom-color: rgba(255, 255, 255, 0.04);
}
th {
  font-weight: 700;
  color: var(--vp-c-text-2);
  background: var(--vp-c-bg-mute);
  text-transform: uppercase;
  font-size: 0.75rem;
  letter-spacing: 0.05em;
}
.dark th {
  background: #111015;
  color: #888;
}
tr:last-child td {
  border-bottom: none;
}
tr:hover td {
  background: rgba(0, 0, 0, 0.02);
}
.dark tr:hover td {
  background: rgba(255, 255, 255, 0.02);
}
.numeric {
  text-align: right;
  font-variant-numeric: tabular-nums;
  font-family: var(--vp-font-family-mono);
}
.highlight-val {
  font-weight: 700;
  color: var(--vp-c-text-1);
}
.dark .highlight-val {
  color: #fff;
}
.highlight-row {
  background: rgba(251, 146, 60, 0.05);
}
.dark .highlight-row {
  background: rgba(251, 146, 60, 0.08);
}
.highlight-row:hover td {
  background: rgba(251, 146, 60, 0.08) !important;
}
.dark .highlight-row:hover td {
  background: rgba(251, 146, 60, 0.12) !important;
}
.tool-badge {
  display: inline-block;
  padding: 0.3rem 0.6rem;
  border-radius: 6px;
  font-weight: 700;
  font-size: 0.75rem;
  background: var(--vp-c-bg-mute);
  color: var(--vp-c-text-2);
  border: 1px solid var(--vp-c-divider);
}
.dark .tool-badge {
  background: #25252d;
  color: #a3a3a3;
  border-color: rgba(255, 255, 255, 0.05);
}
.tool-badge.oxide {
  background: var(--vp-c-brand-soft);
  color: var(--vp-c-brand-1);
  border-color: transparent;
}
.dark .tool-badge.oxide {
  background: rgba(251, 146, 60, 0.15);
  color: #fb923c;
  border-color: rgba(251, 146, 60, 0.3);
}
.mode-badge {
  font-size: 0.7rem;
  text-transform: uppercase;
  letter-spacing: 0.1em;
  color: var(--vp-c-text-2);
  font-weight: 700;
}
.dark .mode-badge {
  color: #777;
}
</style>
