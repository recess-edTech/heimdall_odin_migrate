/* eslint-disable no-unused-vars */
import styles from './Header.module.scss'


import React from 'react'
import { Link, NavLink } from 'react-router-dom'
import { BiSolidCartDownload } from 'react-icons/bi'
import { AiOutlineMenuFold } from 'react-icons/ai'
import { FaTimes } from 'react-icons/fa'

export const Logo = () => {
  return (
    <div className={styles.logo}>
      <Link to='/'>
        <h2>
          Shop<span>Yangu</span>
        </h2>
      </Link>
    </div>
  )
}

const activeLink = ({ isCurrent }) => (isCurrent ? { className: styles.active } : null)

// const activeLink = ({isActive}) => (isActive ? `${styles.active}` : "");
const Header = () => {

  const [showMenu, setShowMenu] = React.useState(false);

  const toggleMenu = () => {
    setShowMenu(!showMenu);
  }

  const hideMenu = () => {
    setShowMenu(false);
  }

  const cart = (
    <span className={styles.cart}>
      <Link to='/cart'>
        Cart
        <BiSolidCartDownload size={20} />
        <p>0</p>
      </Link>
    </span>
  )
  return (
    <header>
      <div className={styles.header}>
        <Logo />

        <nav className={showMenu ? `${styles["show-nav"]}` : `${styles["hide-nav"]}`}>

          <div className={showMenu ? `${styles["nav-wrapper"]} ${styles["show-nav-wrapper"]} ` : `${styles["nav-wrapper"]}`} onClick={hideMenu}>

          </div>

          <ul>
            <li className={styles['logo-mobile']}>
              <Logo />
              <FaTimes size={28} onClick={toggleMenu} />
            </li>
            <li>
              <NavLink to="/shop" className={activeLink} >
                Shop
              </NavLink>
            </li>
          </ul>

          <div className={styles["header-right"]}>
            <span className={styles.links}>

              <NavLink to={'login'} className={activeLink}>
                Login
              </NavLink>

              <NavLink to={'register'} className={activeLink}>
                Register
              </NavLink>

              <NavLink to={'order-history'} className={activeLink}>
                Order History
              </NavLink>
            </span>
            {cart}
          </div>
        </nav>
        <div className={styles["menu-icon"]}>
          <AiOutlineMenuFold size={28} onClick={toggleMenu} />
        </div>
      </div>
    </header>
  )
}

export default Header
