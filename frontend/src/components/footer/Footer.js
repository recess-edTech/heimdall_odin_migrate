import React from 'react'

import styles from './Footer.module.scss'


const Footer = () => {
    const year = new Date().getFullYear();

  return (
      <div className={styles.footer}>  
        &copy; {year} ShopYangu
    </div>
  )
}

export default Footer
