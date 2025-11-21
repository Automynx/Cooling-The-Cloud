import React from 'react';
import { motion } from 'framer-motion';
import { FaPlay, FaArrowRight } from 'react-icons/fa';
import CountUp from 'react-countup';
import { useNavigate } from 'react-router-dom';

function Hero() {
  const navigate = useNavigate();

  const containerVariants = {
    hidden: { opacity: 0 },
    visible: {
      opacity: 1,
      transition: {
        staggerChildren: 0.2,
        delayChildren: 0.3
      }
    }
  };

  const itemVariants = {
    hidden: { y: 20, opacity: 0 },
    visible: {
      y: 0,
      opacity: 1,
      transition: { duration: 0.5 }
    }
  };

  return (
    <section className="relative min-h-screen flex items-center justify-center px-6 pt-20">
      {/* Animated Background Gradient */}
      <div className="absolute inset-0">
        <div className="absolute inset-0 bg-gradient-to-br from-primary-600/20 via-transparent to-accent-600/20 animate-gradient" />
      </div>

      {/* Content */}
      <motion.div
        className="container mx-auto text-center z-10"
        variants={containerVariants}
        initial="hidden"
        animate="visible"
      >
        {/* Badge */}
        <motion.div
          variants={itemVariants}
          className="inline-flex items-center space-x-2 px-4 py-2 rounded-full glass-effect mb-8"
        >
          <span className="relative flex h-2 w-2">
            <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-green-400 opacity-75"></span>
            <span className="relative inline-flex rounded-full h-2 w-2 bg-green-500"></span>
          </span>
          <span className="text-white/80 text-sm font-medium">
            2025 IISE x Think. Solve. Hack
          </span>
        </motion.div>

        {/* Main Headline */}
        <motion.h1
          variants={itemVariants}
          className="text-6xl md:text-8xl font-bold text-white mb-6"
        >
          Cooling the
          <span className="text-gradient block">Cloud</span>
        </motion.h1>

        {/* Subheadline */}
        <motion.p
          variants={itemVariants}
          className="text-xl md:text-2xl text-white/80 max-w-3xl mx-auto mb-8"
        >
          Revolutionary AI-powered optimization reducing Arizona data center costs by{' '}
          <span className="text-accent-400 font-bold">
            <CountUp end={11} duration={2} />%
          </span>{' '}
          while saving millions of gallons of water
        </motion.p>

        {/* Stats */}
        <motion.div
          variants={itemVariants}
          className="grid grid-cols-1 md:grid-cols-3 gap-8 max-w-4xl mx-auto mb-12"
        >
          <div className="glass-effect rounded-2xl p-6">
            <div className="text-3xl font-bold text-primary-400">
              $<CountUp end={41} duration={2} />M
            </div>
            <div className="text-white/60 mt-2">Annual Savings Potential</div>
          </div>
          <div className="glass-effect rounded-2xl p-6">
            <div className="text-3xl font-bold text-accent-400">
              <CountUp end={156} duration={2} />M
            </div>
            <div className="text-white/60 mt-2">Gallons Water Saved/Year</div>
          </div>
          <div className="glass-effect rounded-2xl p-6">
            <div className="text-3xl font-bold text-green-400">
              <CountUp end={26} duration={2} />K
            </div>
            <div className="text-white/60 mt-2">Tons CO₂ Reduced/Year</div>
          </div>
        </motion.div>

        {/* CTA Buttons */}
        <motion.div
          variants={itemVariants}
          className="flex flex-col sm:flex-row gap-4 justify-center"
        >
          <motion.button
            className="btn-primary flex items-center justify-center space-x-2"
            whileHover={{ scale: 1.05 }}
            whileTap={{ scale: 0.95 }}
            onClick={() => navigate('/demo')}
          >
            <FaPlay className="w-4 h-4" />
            <span>Live Demo</span>
          </motion.button>
          <motion.button
            className="btn-secondary flex items-center justify-center space-x-2"
            whileHover={{ scale: 1.05 }}
            whileTap={{ scale: 0.95 }}
            onClick={() => {
              document.getElementById('problem')?.scrollIntoView({ behavior: 'smooth' });
            }}
          >
            <span>Learn More</span>
            <FaArrowRight className="w-4 h-4" />
          </motion.button>
        </motion.div>
      </motion.div>

      {/* Scroll Indicator */}
      <motion.div
        className="absolute bottom-8 left-1/2 transform -translate-x-1/2"
        animate={{ y: [0, 10, 0] }}
        transition={{ repeat: Infinity, duration: 2 }}
      >
        <div className="w-6 h-10 border-2 border-white/30 rounded-full flex justify-center">
          <div className="w-1 h-3 bg-white/60 rounded-full mt-2" />
        </div>
      </motion.div>
    </section>
  );
}

export default Hero;