function generateHash(length) {
  const chars = '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ';
  let hash = '';
  for (let i = 0; i < length; i++) {
    const randIndex = Math.floor(Math.random() * chars.length);
    hash += chars[randIndex];
  }
  return hash;
}

const participantID = generateHash(6);
console.log(participantID); // Outputs a random 6-character alphanumeric string