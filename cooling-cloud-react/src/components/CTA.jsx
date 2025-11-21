import React from 'react';
import { motion } from 'framer-motion';
import { useInView } from 'react-intersection-observer';
import { FaRocket, FaCalendar, FaEnvelope, FaArrowRight } from 'react-icons/fa';

function CTA() {
  const [ref, inView] = useInView({
    triggerOnce: true,
    threshold: 0.1
  });

  return (
    <section className="relative py-32 px-6">
      <div className="container mx-auto">
        <motion.div
          ref={ref}
          initial={{ opacity: 0, scale: 0.95 }}
          animate={inView ? { opacity: 1, scale: 1 } : {}}
          transition={{ duration: 0.8 }}
          className="relative"
        >
          {/* Background Gradient */}
          <div className="absolute inset-0 bg-gradient-to-r from-primary-600 to-accent-600 rounded-3xl blur-3xl opacity-30" />

          {/* Content */}
          <div className="relative glass-effect rounded-3xl p-12 md:p-20 text-center">
            {/* Icon */}
            <motion.div
              initial={{ scale: 0 }}
              animate={inView ? { scale: 1 } : {}}
              transition={{ type: "spring", delay: 0.2 }}
              className="inline-flex items-center justify-center w-20 h-20 rounded-full bg-gradient-to-r from-primary-500 to-accent-500 mb-8"
            >
              <FaRocket className="w-10 h-10 text-white" />
            </motion.div>

            {/* Headline */}
            <motion.h2
              initial={{ opacity: 0, y: 20 }}
              animate={inView ? { opacity: 1, y: 0 } : {}}
              transition={{ delay: 0.3 }}
              className="text-4xl md:text-6xl font-bold text-white mb-6"
            >
              Ready to Save Millions?
            </motion.h2>

            {/* Subheadline */}
            <motion.p
              initial={{ opacity: 0, y: 20 }}
              animate={inView ? { opacity: 1, y: 0 } : {}}
              transition={{ delay: 0.4 }}
              className="text-xl text-white/80 max-w-2xl mx-auto mb-12"
            >
              Join the revolution in data center optimization. Implementation takes just 60 days
              with zero hardware changes required.
            </motion.p>

            {/* CTA Buttons */}
            <motion.div
              initial={{ opacity: 0, y: 20 }}
              animate={inView ? { opacity: 1, y: 0 } : {}}
              transition={{ delay: 0.5 }}
              className="flex flex-col sm:flex-row gap-4 justify-center mb-12"
            >
              <motion.button
                whileHover={{ scale: 1.05 }}
                whileTap={{ scale: 0.95 }}
                className="btn-primary flex items-center justify-center space-x-2"
                onClick={() => window.location.href = '/demo'}
              >
                <FaRocket className="w-5 h-5" />
                <span>Launch Demo</span>
              </motion.button>
              <motion.button
                whileHover={{ scale: 1.05 }}
                whileTap={{ scale: 0.95 }}
                className="btn-secondary flex items-center justify-center space-x-2"
                onClick={() => {
                  const subject = "Consultation Request - Cooling the Cloud Optimization";
                  const body = "Hi Team,\n\nI'm interested in learning more about your data center optimization solution presented at the 2025 IISE Hackathon.\n\nI would like to schedule a consultation to discuss:\n- Implementation for our data center\n- Cost savings potential\n- Water conservation metrics\n- Timeline for deployment\n\nBest regards,\n[Your Name]";
                  const emails = "edarasrimaansri@gmail.com,taimur.adam1@gmail.com,aryanas5426@gmail.com";
                  window.location.href = `mailto:${emails}?subject=${encodeURIComponent(subject)}&body=${encodeURIComponent(body)}`;
                }}
              >
                <FaCalendar className="w-5 h-5" />
                <span>Schedule Consultation</span>
              </motion.button>
            </motion.div>

            {/* Stats Bar */}
            <motion.div
              initial={{ opacity: 0 }}
              animate={inView ? { opacity: 1 } : {}}
              transition={{ delay: 0.6 }}
              className="grid grid-cols-1 md:grid-cols-3 gap-8 pt-12 border-t border-white/20"
            >
              <div>
                <div className="text-2xl font-bold text-white">24/7</div>
                <div className="text-white/60">Automated Optimization</div>
              </div>
              <div>
                <div className="text-2xl font-bold text-white">0</div>
                <div className="text-white/60">Hardware Changes</div>
              </div>
              <div>
                <div className="text-2xl font-bold text-white">60 Days</div>
                <div className="text-white/60">ROI Timeline</div>
              </div>
            </motion.div>
          </div>
        </motion.div>

        {/* Footer */}
        <motion.div
          initial={{ opacity: 0 }}
          animate={inView ? { opacity: 1 } : {}}
          transition={{ delay: 0.8 }}
          className="mt-20 text-center"
        >
          <div className="flex flex-col md:flex-row items-center justify-between gap-8">
            {/* Copyright */}
            <div className="text-white/60">
              © 2025 Cooling the Cloud. IISE Hackathon Project.
            </div>

            {/* Links */}
            <div className="flex items-center space-x-8">
              <a href="#" className="text-white/60 hover:text-white transition">
                Documentation
              </a>
              <a href="https://github.com/Automynx/Cooling-The-Cloud" target="_blank" rel="noopener noreferrer" className="text-white/60 hover:text-white transition">
                GitHub
              </a>
              <a
                href="mailto:edarasrimaansri@gmail.com,taimur.adam1@gmail.com,aryanas5426@gmail.com?subject=Inquiry - Cooling the Cloud Project"
                className="text-white/60 hover:text-white transition"
              >
                Contact
              </a>
            </div>

            {/* Contact */}
            <a
              href="mailto:edarasrimaansri@gmail.com,taimur.adam1@gmail.com,aryanas5426@gmail.com"
              className="flex items-center space-x-2 text-white/60 hover:text-white transition"
            >
              <FaEnvelope className="w-4 h-4" />
              <span>edarasrimaansri@gmail.com</span>
            </a>
          </div>
        </motion.div>
      </div>
    </section>
  );
}

export default CTA;