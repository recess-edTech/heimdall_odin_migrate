//eslint-disable-next-line
import React from 'react'
import Home from './pages/Home';
import Header from './components/header/Header';
import Footer from './components/footer/Footer';

import { BrowserRouter, Routes, Route } from 'react-router-dom'


const App = () => {
  return (
    <>
      <BrowserRouter>
        <Header />
        <Routes>
          <Route path="/" element={<Home />} />
        </Routes>
        <Footer />
      </BrowserRouter>
    </>
  )
}

export default App
