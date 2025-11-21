import React from 'react';
import { motion } from 'framer-motion';
import { FaCloud } from 'react-icons/fa';

function Navigation({ isScrolled }) {
  return (
    <motion.nav
      className={`fixed top-0 left-0 right-0 z-50 transition-all duration-300 ${
        isScrolled ? 'glass-effect py-4' : 'py-6'
      }`}
      initial={{ y: -100 }}
      animate={{ y: 0 }}
      transition={{ duration: 0.5 }}
    >
      <div className="container mx-auto px-6 flex items-center justify-between">
        {/* Logo */}
        <motion.div
          className="flex items-center space-x-3"
          whileHover={{ scale: 1.05 }}
        >
          <div className="relative">
            <FaCloud className="w-8 h-8 text-primary-400" />
            <div className="absolute inset-0 w-8 h-8 bg-primary-400 blur-xl opacity-50 animate-pulse" />
          </div>
          <span className="text-xl font-bold text-white">
            Cooling<span className="text-gradient">Cloud</span>
          </span>
        </motion.div>

        {/* Navigation Links */}
        <div className="hidden md:flex items-center space-x-8">
          <a href="#problem" className="text-white/80 hover:text-white transition">
            Problem
          </a>
          <a href="#solution" className="text-white/80 hover:text-white transition">
            Solution
          </a>
          <a href="#impact" className="text-white/80 hover:text-white transition">
            Impact
          </a>
          <a href="#technology" className="text-white/80 hover:text-white transition">
            Technology
          </a>
        </div>

        {/* CTA Button */}
        <motion.button
          className="btn-primary text-sm"
          whileHover={{ scale: 1.05 }}
          whileTap={{ scale: 0.95 }}
          onClick={() => window.location.href = '/demo'}
        >
          View Demo
        </motion.button>
      </div>
    </motion.nav>
  );
}

export default Navigation;