import React from 'react';


function Layout({children}: {children:  React.ReactNode}) {
    return (
        <div className="h-full flex items-center justify-center bg-[radial-gradient(ellipse_at_top,_var(--tw-gradient-stops))] from-sky-400 to-black">
            {children}
        </div>
    );
}

export default Layout;
