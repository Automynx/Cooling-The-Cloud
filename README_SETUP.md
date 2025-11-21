# Quick Setup Guide - Cooling the Cloud

## Overview
This project optimizes Arizona data center operations by minimizing costs through intelligent load shifting and cooling system management. The **React frontend** is the main user interface for the optimization system.

## Prerequisites
- Node.js (v16 or higher)
- npm (comes with Node.js)
- Python 3.8+ (for backend API)
- GLPK solver (for optimization)

## Quick Start

### 1. Install Dependencies

#### Frontend (React) - Main Interface
```bash
cd cooling-cloud-react
npm install
```

#### Backend (Python)
```bash
# From the root directory
pip install -r requirements.txt
```

### 2. Run the Application

#### Start the React Frontend (Main Interface)
```bash
cd cooling-cloud-react
npm run dev
```
The React app will start at **http://localhost:5173/** (or port 3001 if 5173 is in use)

This is the **primary user interface** with:
- Interactive dashboard for live optimization demo
- Real-time parameter adjustments
- Cost savings visualizations
- Water usage monitoring
- Load shifting controls

#### Backend API Server (Optional - for full functionality)
```bash
# From the root directory
python api_server.py
```
The API server runs on port 5000 and handles optimization calculations.

## Important Notes

- **React Frontend**: This is the main interface users should interact with. It provides a modern, responsive UI for the optimization system.
- **Streamlit App**: We do NOT use the Streamlit interface (`streamlit_app.py`) - the React frontend has replaced it entirely.
- **Live Demo**: The React dashboard includes a fully functional live demo with real Phoenix data.

## Features Available in React Frontend

- **Real-time Optimization**: Adjust parameters and see immediate cost/water impact
- **Load Shifting Visualization**: Interactive charts showing computational load distribution
- **Cooling System Control**: Switch between water-based and electric cooling modes
- **Cost Analysis**: Track electricity costs, water usage, and total savings
- **Phoenix-Specific Data**: Uses actual Arizona utility rates and weather patterns

## Troubleshooting

### Port Already in Use
If you see "Port 5173 is in use", the dev server will automatically try port 3001 or another available port.

### Dependencies Not Found
If you get "vite: command not found" or similar errors:
```bash
cd cooling-cloud-react
npm install  # Reinstall dependencies
npm run dev
```

### GLPK Solver Issues
If optimization fails, ensure GLPK is installed:
- Mac: `brew install glpk`
- Ubuntu/WSL: `sudo apt-get install glpk-utils`
- Windows: Download from https://www.gnu.org/software/glpk/

## Development

The React frontend automatically hot-reloads when you make changes. The main entry point is `cooling-cloud-react/src/App.jsx`.

For production deployment:
```bash
cd cooling-cloud-react
npm run build
```
This creates an optimized build in the `dist/` directory.