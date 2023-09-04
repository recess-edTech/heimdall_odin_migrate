


export const shortenText = (text, limit) => {
    if(text.length > limit){
        return text.slice(0, limit) + '...'
    }
    return text
}